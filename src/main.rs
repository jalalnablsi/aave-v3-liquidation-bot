// =================================================================================================
// 💀 GRIM REAPER X - ULTIMATE FUSION EDITION (MAX SPEED + STABILITY) 💀
// =================================================================================================
// يجمع بين:
// - تحميل CSV وتحديثات فائقة السرعة عبر Multicall3 (من النسخة الأولى).
// - تحديث فوري للمستخدمين المتأثرين عند تغير أي سعر (Price-Triggered).
// - آليات تصفية ذكية وآمنة: محاكاة مع تايم أوت، كميات احتياطية 50% (من النسخة الثانية).
// - إدارة متقدمة للقائمة السوداء وفشل المحاولات.
// - استخدام Multicall3 لجلب تفاصيل الأصول بكفاءة.
// - مزودون احتياطيون، إعادة اتصال WebSocket أُسّي.
// =================================================================================================

use alloy::{
    network::EthereumWallet,
    primitives::{address, Address, U256, B256, I256, Bytes},
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    rpc::types::eth::{Filter, Log, TransactionRequest},
    signers::local::PrivateKeySigner,
    transports::http::{Client, Http},
};
use anyhow::{bail, Result};
use alloy_sol_types::SolCall;
use dashmap::DashMap;
use dotenv::dotenv;
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::Path,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{broadcast, mpsc, Semaphore, Mutex},
    time,
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// =================================================================================================
// الثوابت والعناوين (Polygon Mainnet)
// =================================================================================================
const AAVE_POOL: Address = address!("794a61358D6845594F94dc1DB02A252b5b4814aD");
const AAVE_DATA_PROVIDER: Address = address!("0x243Aa95cAC2a25651eda86e80bEe66114413c43b");
const MULTICALL3: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");

const WMATIC: Address = address!("0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270");
const WETH: Address = address!("7ceB23fD6bC0adD59E62ac25578270cFf1b9f619");
const WBTC: Address = address!("1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6");
const USDC: Address = address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
const USDT: Address = address!("c2132D05D31c914a87C6611C10748AEb04B58e8F");
const DAI: Address = address!("8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063");

const MATIC_USD_FEED: Address = address!("AB594600376Ec9fD91F8e885dADF0CE036862dE0");
const ETH_USD_FEED: Address = address!("F9680D99D6C9589e2a93a78A04A279e509205945");
const BTC_USD_FEED: Address = address!("c907E116054Ad103354f2D350FD2514433D57F6f");
const USDC_USD_FEED: Address = address!("572dDec9087154dC5dfBB1546Bb62713147e0Ab0");
const USDT_USD_FEED: Address = address!("0A6513e40db6EB1b165753AD52E80663aeA50545");
const DAI_USD_FEED: Address = address!("4746DeC9e833A82EC7C2C1356372CcF2cfcD2f3D");

const ANSWER_UPDATED_SIG: B256 = B256::new(hex_literal::hex!(
    "0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
));
const BORROW_EVENT_SIG: B256 = B256::new(hex_literal::hex!(
    "c6a898309e823ee50bac64e5ca7099b8c577d447dc0832b5c6f4da284a7a1fe5"
));
const REPAY_EVENT_SIG: B256 = B256::new(hex_literal::hex!(
    "b718f0b14f03d8c3adf35ce30845a4c1642d8f74102f0de73ed3b0f1b0e3e3b2" // الصحيح
));
const LIQUIDATION_EVENT_SIG: B256 = B256::new(hex_literal::hex!(
    "e413a321e8681d831f20dbccbca09d0082e688e6a885353c73e9b85f1ed1b0c0"
));

const MAX_CONCURRENT_RPC: usize = 50;
const MAX_CONCURRENT_LIQUIDATIONS: usize = 3;
const PRICE_HISTORY_SIZE: usize = 100;
const CSV_FILE_PATH: &str = "borrowers.csv";
const MIN_PROFIT_USD: f64 = 1.0;
const GAS_LIMIT: u64 = 3_000_000; 
const GAS_PREMIUM_PERCENT: u64 = 20;
const BATCH_SIZE: usize = 15;   // لحجم دفعات Multicall
const UPDATE_BATCH_INTERVAL_MS: u64 = 100;

const SUPPORTED_COLLATERAL: &[(Address, Address, u8, f64)] = &[
    (WMATIC, MATIC_USD_FEED, 8, 0.05),
    (WETH, ETH_USD_FEED, 8, 0.05),
    (WBTC, BTC_USD_FEED, 8, 0.03),
];
const SUPPORTED_DEBT: &[(Address, Address, u8)] = &[
    (USDC, USDC_USD_FEED, 6),
    (USDT, USDT_USD_FEED, 6),
    (DAI, DAI_USD_FEED, 18),
];

// ============================================================================
// تعريف العقود
// ============================================================================
alloy::sol! {
    #[sol(rpc)]
    contract GrimReaperV6 {
        function executeFlashLiquidation(
            address collateralAsset,
            address debtAsset,
            address user,
            uint256 debtAmount,
            uint256 minProfit
        ) external returns (bool);
        function withdraw(address token) external;
        function setupApprovals() external;
    }

    #[sol(rpc)]
    interface IAavePool {
        function getUserAccountData(address user) external view returns (
            uint256 totalCollateralBase,
            uint256 totalDebtBase,
            uint256 availableBorrowsBase,
            uint256 currentLiquidationThreshold,
            uint256 ltv,
            uint256 healthFactor
        );
    }

    #[sol(rpc)]
    interface IAaveProtocolDataProvider {
        function getUserReserveData(address asset, address user) external view returns (
            uint256 currentATokenBalance,
            uint256 currentStableDebt,
            uint256 currentVariableDebt,
            uint256 principalStableDebt,
            uint256 scaledVariableDebt,
            uint256 stableBorrowRate,
            uint256 liquidityRate,
            uint40 stableRateLastUpdated,
            bool usageAsCollateralEnabled
        );
    }

    #[sol(rpc)]
    interface IAggregatorV3 {
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
    }

    #[sol(rpc)]
    contract Multicall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    }

    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function decimals() external view returns (uint8);
    }
}

// ============================================================================
// دوال مساعدة
// ============================================================================
fn u256_to_f64(val: U256, decimals: u32) -> f64 {
    if val == U256::MAX { return f64::MAX; }
    let divisor = U256::from(10u128.pow(decimals));
    let int = val / divisor;
    let rem = val % divisor;
    int.to::<u64>() as f64 + (rem.to::<u64>() as f64 / 10f64.powi(decimals as i32))
}

fn i256_to_f64(value: I256, decimals: u8) -> f64 {
    if value.is_zero() { return 0.0; }
    if value.is_negative() { return 0.0; }
    let raw = value.into_raw();
    let divisor = U256::from(10).pow(U256::from(decimals));
    let int_part = raw / divisor;
    let rem_part = raw % divisor;
    let int_f64 = int_part.to::<u64>() as f64;
    let rem_f64 = rem_part.to::<u64>() as f64 / 10f64.powi(decimals as i32);
    int_f64 + rem_f64
}

fn float_to_uint(value: f64, decimals: u8) -> U256 {
    // نضرب القيمة في 10^decimals باستخدام النص لضمان الدقة
    let multiplier = 10f64.powi(decimals as i32);
    let scaled = value * multiplier;
    if scaled >= 1e30 { // أمان من التجاوزات الكبيرة
        return U256::MAX;
    }
    let int_representation = scaled.round() as i128; // لأخذ أقرب عدد صحيح
    if int_representation < 0 {
        U256::ZERO
    } else {
        U256::from(int_representation as u128)
    }
}

// ============================================================================
// هياكل البيانات
// ============================================================================
#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub asset: Address,
    pub price: f64,
    pub timestamp: u64,
    pub source: PriceSource,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriceSource {
    WebSocket,
    RPC,
    Cached,
}

#[derive(Debug, Clone)]
pub struct UserScore {
    pub address: Address,
    pub health_factor: f64,
    pub total_debt_usd: f64,
    pub total_collateral_usd: f64,
    pub liquidation_threshold: f64,
    pub liquidation_bonus: f64,
    pub priority: Priority,
    pub score: f64,
    pub last_update: Instant,
    pub debt_assets: HashMap<Address, U256>,       // أصول الدين وكمياتها الفعلية
    pub collateral_assets: HashMap<Address, U256>, // أصول الضمان وكمياتها
    pub liquidation_profit_estimate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Critical = 0,
    Imminent = 1,
    High = 2,
    Medium = 3,
    Low = 4,
    Watching = 5,
}

impl UserScore {
    fn is_liquidatable(&self) -> bool {
        self.health_factor < 1.0 && self.health_factor > 0.0 && self.total_debt_usd > 10.0
    }

    fn calculate_score(&mut self) {
        let hf_score = if self.health_factor <= 1.0 { 1000.0 }
                       else if self.health_factor < 1.02 { 500.0 / self.health_factor }
                       else if self.health_factor < 1.05 { 200.0 / self.health_factor }
                       else { 50.0 / self.health_factor };
        let debt_score = (self.total_debt_usd / 100.0).min(200.0);
        let profit_score = self.liquidation_profit_estimate * 10.0;
        self.score = hf_score + debt_score + profit_score;
        self.priority = if self.health_factor < 1.0 { Priority::Critical }
                        else if self.health_factor < 1.02 { Priority::Imminent }
                        else if self.health_factor < 1.05 { Priority::High }
                        else if self.health_factor < 1.10 { Priority::Medium }
                        else if self.health_factor < 1.20 { Priority::Low }
                        else { Priority::Watching };
    }

    fn estimate_profit(&self) -> f64 {
        self.liquidation_profit_estimate
    }

    fn calculate_liquidation_profit(&mut self) {
        if !self.is_liquidatable() {
            self.liquidation_profit_estimate = 0.0;
            return;
        }
        let max_liquidatable = self.total_debt_usd * 0.5;
        let base_profit = max_liquidatable * self.liquidation_bonus;
        let estimated_gas = 0.05; // بالدولار
        self.liquidation_profit_estimate = (base_profit - estimated_gas).max(0.0);
    }
}

// ============================================================================
// مدير الأسعار المتقدم (مع السجل والتقلب)
// ============================================================================
pub struct PriceManager {
    prices: Arc<DashMap<Address, PriceUpdate>>,
    history: Arc<DashMap<Address, VecDeque<PriceUpdate>>>,
    price_update_tx: broadcast::Sender<PriceUpdate>,
    asset_to_users: Arc<DashMap<Address, HashSet<Address>>>,
    volatility: Arc<DashMap<Address, f64>>,
}

impl PriceManager {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(2048);
        Self {
            prices: Arc::new(DashMap::new()),
            history: Arc::new(DashMap::new()),
            price_update_tx: tx,
            asset_to_users: Arc::new(DashMap::new()),
            volatility: Arc::new(DashMap::new()),
        }
    }

    pub fn update_price(&self, asset: Address, price: f64, source: PriceSource) {
        if price <= 0.0 || price.is_nan() || price.is_infinite() { return; }
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let update = PriceUpdate { asset, price, timestamp, source };
        self.prices.insert(asset, update.clone());
        // تحديث السجل
        let mut hist = self.history.entry(asset).or_insert_with(VecDeque::new);
        hist.push_back(update.clone());
        if hist.len() > PRICE_HISTORY_SIZE { hist.pop_front(); }
        // حساب التقلب
        if hist.len() >= 10 {
            let returns: Vec<f64> = hist.iter().collect::<Vec<_>>().windows(2)
                .map(|w| (w[1].price - w[0].price) / w[0].price).collect();
            if !returns.is_empty() {
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
                let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
                let vol = variance.sqrt();
                self.volatility.insert(asset, vol);
            }
        }
        let _ = self.price_update_tx.send(update);
    }

    pub fn get_price(&self, asset: Address) -> Option<f64> {
        self.prices.get(&asset).map(|p| p.price)
    }

    pub fn get_all_prices(&self) -> HashMap<Address, f64> {
        self.prices.iter().map(|e| (*e.key(), e.value().price)).collect()
    }

    pub fn get_volatility(&self, asset: Address) -> f64 {
        self.volatility.get(&asset).map(|v| *v).unwrap_or(0.01)
    }

    pub fn subscribe_prices(&self) -> broadcast::Receiver<PriceUpdate> {
        self.price_update_tx.subscribe()
    }

    pub fn register_user_asset(&self, user: Address, asset: Address) {
        self.asset_to_users.entry(asset).or_insert_with(HashSet::new).insert(user);
    }

    pub fn get_affected_users(&self, asset: Address) -> Vec<Address> {
        self.asset_to_users.get(&asset).map(|users| users.iter().cloned().collect()).unwrap_or_default()
    }
}

// ============================================================================
// التخزين المؤقت الذكي
// ============================================================================
#[derive(Clone)]
pub struct SmartCache {
    pub users: Arc<DashMap<Address, UserScore>>,
    pub critical_users: Arc<DashMap<Address, UserScore>>,
    blacklist: Arc<DashMap<Address, Instant>>,
    liquidation_attempts: Arc<DashMap<Address, AtomicUsize>>,
}

impl SmartCache {
    pub fn new() -> Self {
        Self {
            users: Arc::new(DashMap::new()),
            critical_users: Arc::new(DashMap::new()),
            blacklist: Arc::new(DashMap::new()),
            liquidation_attempts: Arc::new(DashMap::new()),
        }
    }

    pub fn update_user(&self, mut score: UserScore) {
    if score.total_debt_usd == 0.0 {
        self.users.remove(&score.address);
        self.critical_users.remove(&score.address);
        return;
    }
    score.calculate_score();
    score.calculate_liquidation_profit();

    // تحديث user (باستخدام entry لمنع over-write من خيط آخر)
    self.users.entry(score.address)
        .and_modify(|existing| *existing = score.clone())
        .or_insert(score.clone());

    // تحديث critical بناءً على الأولوية الجديدة
    match score.priority {
        Priority::Critical | Priority::Imminent => {
            self.critical_users.insert(score.address, score);
        }
        _ => {
            // إزالة من critical إذا كان health_factor > 1.05
            if score.health_factor > 1.05 {
                self.critical_users.remove(&score.address);
            }
        }
    }
}
    pub fn get_top_targets(&self, limit: usize) -> Vec<UserScore> {
        let mut targets: Vec<UserScore> = self.critical_users.iter()
            .map(|e| e.value().clone())
            .filter(|s| s.is_liquidatable() && !self.is_blacklisted(&s.address))
            .collect();
        targets.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        targets.truncate(limit);
        targets
    }

    pub fn mark_liquidated(&self, user: Address) {
        self.critical_users.remove(&user);
        self.blacklist.insert(user, Instant::now() + Duration::from_secs(300));
        self.liquidation_attempts.entry(user)
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_failed_attempt(&self, user: Address) {
        let attempts = self.liquidation_attempts
            .entry(user)
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
        if attempts >= 3 {
            self.blacklist.insert(user, Instant::now() + Duration::from_secs(3600));
            self.critical_users.remove(&user);
            info!("🚫 المستخدم {:?} حُظر لمدة ساعة بعد 3 محاولات فاشلة", user);
        }
    }

    pub fn is_blacklisted(&self, user: &Address) -> bool {
        if let Some(exp) = self.blacklist.get(user) {
            if *exp.value() > Instant::now() { return true; }
        }
        false
    }

    pub fn cleanup(&self) {
        self.blacklist.retain(|_, exp| *exp > Instant::now());
        self.users.retain(|_, score| score.last_update.elapsed() < Duration::from_secs(1800));
    }

    pub fn total_users(&self) -> usize { self.users.len() }
    pub fn critical_count(&self) -> usize { self.critical_users.len() }
}

// ============================================================================
// المُصفي المتقدم
// ============================================================================
pub struct AdvancedLiquidator {
    read_provider: Arc<RootProvider<Http<Client>>>,
    write_provider: RootProvider<Http<Client>>,
    backup_providers: Vec<RootProvider<Http<Client>>>,
    contract_addr: Address,
    signer: PrivateKeySigner,
    cache: SmartCache,
    price_manager: Arc<PriceManager>,
    liquidation_semaphore: Arc<Semaphore>,
    rpc_semaphore: Arc<Semaphore>,
    min_profit_usd: f64,
    stats: Arc<LiquidatorStats>,
    gas_price_cache: Arc<Mutex<(u64, Instant)>>,
    token_decimals: HashMap<Address, u8>,
    telegram_token: String,
    telegram_chat: String,
}

#[derive(Debug, Default)]
struct LiquidatorStats {
    attempts: AtomicUsize,
    successes: AtomicUsize,
    failures: AtomicUsize,
}

impl AdvancedLiquidator {
    pub fn new(
        read_provider: Arc<RootProvider<Http<Client>>>,
        write_provider: RootProvider<Http<Client>>,
        contract_addr: Address,
        signer: PrivateKeySigner,
        cache: SmartCache,
        price_manager: Arc<PriceManager>,
        telegram_token: String,
        telegram_chat: String,
        backup_urls: &[String],
    ) -> Self {
        let backup_providers = backup_urls.iter().filter_map(|url| {
            url.parse::<reqwest::Url>().ok().map(|parsed| ProviderBuilder::new().on_http(parsed))
        }).collect();
        let mut token_decimals = HashMap::new();
        token_decimals.insert(USDC, 6);
        token_decimals.insert(USDT, 6);
        token_decimals.insert(DAI, 18);
        token_decimals.insert(WETH, 18);
        token_decimals.insert(WMATIC, 18);
        token_decimals.insert(WBTC, 18);
        Self {
            read_provider,
            write_provider,
            backup_providers,
            contract_addr,
            signer,
            cache,
            price_manager,
            liquidation_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_LIQUIDATIONS)),
            rpc_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_RPC)),
            min_profit_usd: MIN_PROFIT_USD,
            stats: Arc::new(LiquidatorStats::default()),
            gas_price_cache: Arc::new(Mutex::new((0, Instant::now()))),
            token_decimals,
            telegram_token,
            telegram_chat,
        }
    }

    async fn get_gas_price(&self) -> Result<u64> {
        {
            let cache = self.gas_price_cache.lock().await;
            if cache.1.elapsed() < Duration::from_secs(5) { return Ok(cache.0); }
        }
        // حاول المزود الأساسي ثم الاحتياطي
        for provider in std::iter::once(&self.write_provider).chain(self.backup_providers.iter()) {
            if let Ok(gas) = provider.get_gas_price().await {
                let price: u64 = gas.try_into().unwrap_or(100_000_000_000);
                let optimal = price * (100 + GAS_PREMIUM_PERCENT) / 100;
                let mut cache = self.gas_price_cache.lock().await;
                *cache = (optimal, Instant::now());
                return Ok(optimal);
            }
        }
        Ok(100_000_000_000)
    }

    /// جلب جميع ديون المستخدم دفعة واحدة باستخدام Multicall3
    async fn fetch_user_debts_batch(&self, user: Address) -> Result<HashMap<Address, U256>> {
        let data_provider = IAaveProtocolDataProvider::new(AAVE_DATA_PROVIDER, self.read_provider.clone());
        let multicall = Multicall3::new(MULTICALL3, self.read_provider.clone());
        let debt_tokens: Vec<Address> = SUPPORTED_DEBT.iter().map(|d| d.0).collect();
        let mut calls = Vec::new();
        for token in &debt_tokens {
            let calldata = data_provider.getUserReserveData(*token, user).calldata().to_vec();
            calls.push(Multicall3::Call3 {
                target: AAVE_DATA_PROVIDER,
                allowFailure: true,
                callData: Bytes::from(calldata),
            });
        }
        let results = multicall.aggregate3(calls).call().await?;
        let mut debts = HashMap::new();
        for (i, res) in results.returnData.iter().enumerate() {
            if !res.success { continue; }
            if let Ok(data) = IAaveProtocolDataProvider::getUserReserveDataCall::abi_decode_returns(&res.returnData, false) {
                let total_debt = data.currentStableDebt + data.currentVariableDebt;
                if !total_debt.is_zero() {
                    debts.insert(debt_tokens[i], total_debt);
                }
            }
        }
        Ok(debts)
    }

    async fn select_best_liquidation_pair(&self, score: &UserScore) -> Result<(Address, Address, U256)> {
        if !score.debt_assets.is_empty() && !score.collateral_assets.is_empty() {
            // استخدم البيانات التفصيلية إذا كانت متوفرة
            let mut best_pair = None;
            let mut best_profit = 0.0_f64;
            for (&debt_asset, &debt_amount) in &score.debt_assets {
                if debt_amount.is_zero() { continue; }
                for (&collateral_asset, &_coll_amount) in &score.collateral_assets {
                    let bonus = SUPPORTED_COLLATERAL.iter()
                        .find(|(c, _, _, _)| *c == collateral_asset)
                        .map(|(_, _, _, b)| *b)
                        .unwrap_or(0.05);
                    let profit = self.estimate_pair_profit(debt_asset, debt_amount, bonus);
                    if profit > best_profit {
                        best_profit = profit;
                        best_pair = Some((collateral_asset, debt_asset, debt_amount));
                    }
                }
            }
            if let Some(pair) = best_pair {
                return Ok(pair);
            }
        }
        // حل احتياطي: جلب الديون الفعلية من السلسلة (للمستخدمين الذين ليس لديهم تفاصيل)
        let user_debts = self.fetch_user_debts_batch(score.address).await?;
        let mut best_pair = None;
        let mut best_profit = 0.0;
        for (&debt_asset, &debt_amount) in &user_debts {
            for collateral_asset in SUPPORTED_COLLATERAL.iter().map(|c| c.0) {
                let bonus = SUPPORTED_COLLATERAL.iter()
                    .find(|(c, _, _, _)| *c == collateral_asset)
                    .map(|(_, _, _, b)| *b)
                    .unwrap_or(0.05);
                let profit = self.estimate_pair_profit(debt_asset, debt_amount, bonus);
                if profit > best_profit {
                    best_profit = profit;
                    best_pair = Some((collateral_asset, debt_asset, debt_amount));
                }
            }
        }
        best_pair.ok_or_else(|| anyhow::anyhow!("لا يوجد زوج مربح للمستخدم {:?}", score.address))
    }

    fn estimate_pair_profit(&self, debt_asset: Address, debt_amount: U256, bonus: f64) -> f64 {
        let decimals = self.token_decimals.get(&debt_asset).copied().unwrap_or(18);
        let debt_float = u256_to_f64(debt_amount, decimals as u32);
        let price = self.price_manager.get_price(debt_asset).unwrap_or(1.0);
        let debt_usd = debt_float * price;
        let max_liquidatable = debt_usd * 0.5;
        let base_profit = max_liquidatable * bonus;
        (base_profit - 0.05).max(0.0)
    }

    pub async fn liquidate(&self, score: &UserScore) -> Result<Option<B256>> {
    if !score.is_liquidatable() { bail!("غير قابل للتصفية"); }
    if self.cache.is_blacklisted(&score.address) { bail!("محظور"); }

    // ✅ إعادة حساب الربح باستخدام أحدث الأسعار
    let mut fresh_score = score.clone();
    // تحديث السعر من المدير لتقدير الربح
    if let Some(debt_price) = fresh_score.debt_assets.iter().next().map(|(asset, _)| self.price_manager.get_price(*asset)) {
        // (سنكتفي بحساب الربح بناءً على total_debt_usd في time-of-liquidation)
    }
    fresh_score.calculate_liquidation_profit();
    
    if fresh_score.estimate_profit() < self.min_profit_usd {
        bail!("ربح أقل من الحد الأدنى");
    }

    let _permit = self.liquidation_semaphore.acquire().await?;
    self.stats.attempts.fetch_add(1, Ordering::Relaxed);

    let (collateral, debt, mut debt_amount) = match self.select_best_liquidation_pair(&fresh_score).await {
        Ok(p) => p,
        Err(e) => {
            self.stats.failures.fetch_add(1, Ordering::Relaxed);
            self.cache.mark_failed_attempt(fresh_score.address);
            error!("❌ فشلت تصفية {:?}: {}", fresh_score.address, e);
            self.send_telegram(&format!("❌ فشل: {:?}\n{}", fresh_score.address, e)).await;
            bail!(e);
        }
    };

    // ✅ تقييد المبلغ إلى الحد المسموح (50% من إجمالي الدين بقيمة الدولار)
    let debt_price = self.price_manager.get_price(debt).unwrap_or(1.0);
    let debt_decimals = self.token_decimals.get(&debt).copied().unwrap_or(18);
    let max_liquidatable_usd = fresh_score.total_debt_usd * 0.5;
    let max_debt_token = float_to_uint(max_liquidatable_usd / debt_price, debt_decimals);
    debt_amount = debt_amount.min(max_debt_token);

    if debt_amount.is_zero() {
        bail!("المبلغ المطلوب صفر بعد التقييد");
    }

    // minProfit = 0.7 * الربح المقدّر (بالدولار محوّل لوحدات الرمز)
    let min_profit_wei = float_to_uint(
        fresh_score.liquidation_profit_estimate * 0.7,
        debt_decimals,
    );

    // استراتيجيات المبلغ: 100% ثم 50% من المبلغ المعدّل
    let amounts = vec![debt_amount, debt_amount / U256::from(2)];

    for &amount in &amounts {
        if amount.is_zero() { continue; }
        match self.try_liquidation(collateral, debt, fresh_score.address, amount, min_profit_wei).await {
            Ok(Some(hash)) => {
                self.stats.successes.fetch_add(1, Ordering::Relaxed);
                self.cache.mark_liquidated(fresh_score.address);
                self.send_telegram(&format!("✅ نجحت تصفية {:?}\nTX: {:?}", fresh_score.address, hash)).await;
                return Ok(Some(hash));
            }
            Ok(None) => continue,
            Err(e) => { warn!("المحاولة فشلت: {}", e); continue; }
        }
    }

    self.stats.failures.fetch_add(1, Ordering::Relaxed);
    self.cache.mark_failed_attempt(fresh_score.address);
    self.send_telegram(&format!("❌ فشل جميع المحاولات لـ {:?}", fresh_score.address)).await;
    bail!("جميع محاولات التصفية فشلت")
}

    async fn try_liquidation(
    &self,
    collateral: Address,
    debt: Address,
    user: Address,
    amount: U256,
    min_profit: U256,
) -> Result<Option<B256>> {
    let _permit = self.rpc_semaphore.acquire().await?;
    let gas_price = self.get_gas_price().await?;

    // ✅ توليد calldata عبر ABI المُولَّد
    let call = GrimReaperV6::executeFlashLiquidationCall {
        collateralAsset: collateral,
        debtAsset: debt,
        user,
        debtAmount: amount,
        minProfit: min_profit,
    };
    let calldata = call.abi_encode();

    let tx = TransactionRequest {
        to: Some(alloy::primitives::TxKind::Call(self.contract_addr)),
        input: alloy::primitives::Bytes::from(calldata).into(),
        gas: Some(GAS_LIMIT),
        gas_price: Some(gas_price as u128),
        from: Some(self.signer.address()),
        ..Default::default()
    };

    // محاكاة
    match time::timeout(Duration::from_secs(15), self.write_provider.call(&tx)).await {
        Ok(Ok(_)) => {},
        Ok(Err(e)) => bail!("فشل المحاكاة: {:?}", e),
        Err(_) => bail!("انتهت مهلة المحاكاة"),
    }

    // إرسال المعاملة باستخدام WalletProvider مع nonce-manager (نستخدم self.write_provider لكنه RootProvider)
    // هنا ننشئ نسخة محلية من wallet_provider
    let wallet = EthereumWallet::from(self.signer.clone());
    let wallet_provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet)
        .on_provider(self.write_provider.clone());

    let pending = wallet_provider.send_transaction(tx).await?;
    let tx_hash = *pending.tx_hash();
    info!("📤 تم الإرسال: {:?}", tx_hash);

    match time::timeout(Duration::from_secs(60), pending.get_receipt()).await {
        Ok(Ok(receipt)) if receipt.status() => {
            info!("✅ تأكدت: {:?}", tx_hash);
            Ok(Some(tx_hash))
        }
        Ok(Ok(_)) => bail!("تم إرجاع المعاملة"),
        Ok(Err(e)) => bail!("خطأ في الإيصال: {:?}", e),
        Err(_) => bail!("انتهت مهلة التأكيد"),
    }
}

    async fn send_telegram(&self, msg: &str) {
        if self.telegram_token.is_empty() || self.telegram_chat.is_empty() { return; }
        let client = reqwest::Client::new();
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.telegram_token);
        let _ = client.post(&url).form(&[
            ("chat_id", self.telegram_chat.as_str()),
            ("text", msg),
            ("parse_mode", "HTML"),
        ]).timeout(Duration::from_secs(5)).send().await;
    }

    pub fn clone_light(&self) -> Self {
        Self {
            read_provider: self.read_provider.clone(),
            write_provider: self.write_provider.clone(),
            backup_providers: self.backup_providers.clone(),
            contract_addr: self.contract_addr,
            signer: self.signer.clone(),
            cache: self.cache.clone(),
            price_manager: self.price_manager.clone(),
            liquidation_semaphore: self.liquidation_semaphore.clone(),
            rpc_semaphore: self.rpc_semaphore.clone(),
            min_profit_usd: self.min_profit_usd,
            stats: self.stats.clone(),
            gas_price_cache: self.gas_price_cache.clone(),
            token_decimals: self.token_decimals.clone(),
            telegram_token: self.telegram_token.clone(),
            telegram_chat: self.telegram_chat.clone(),
        }
    }

    pub fn get_stats(&self) -> (usize, usize, usize) {
        (
            self.stats.attempts.load(Ordering::Relaxed),
            self.stats.successes.load(Ordering::Relaxed),
            self.stats.failures.load(Ordering::Relaxed),
        )
    }
}

// ============================================================================
// الماسح التفاعلي ذو التحديث الفوري + Multicall
// ============================================================================
pub struct ReactiveScanner {
   read_providers: Vec<RootProvider<Http<Client>>>,
provider_index: AtomicUsize,
    cache: SmartCache,
    price_manager: Arc<PriceManager>,
    liquidator: Arc<AdvancedLiquidator>,
    update_queue: mpsc::UnboundedSender<Address>,
    semaphore: Arc<Semaphore>,
}

impl ReactiveScanner {
    pub fn new(
    rpc_urls: &[String],   // <-- تغيير المعطى الأول
    cache: SmartCache,
    price_manager: Arc<PriceManager>,
    liquidator: Arc<AdvancedLiquidator>,
) -> (Self, mpsc::UnboundedReceiver<Address>) {
    let read_providers: Vec<_> = rpc_urls
        .iter()
        .filter_map(|url| url.parse::<reqwest::Url>().ok())
        .map(|url| ProviderBuilder::new().on_http(url))
        .collect();

    let (tx, rx) = mpsc::unbounded_channel();
    (
        Self {
            read_providers,
            provider_index: AtomicUsize::new(0),   // <-- تهيئة العداد
            cache,
            price_manager,
            liquidator,
            update_queue: tx,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_RPC)),
        },
        rx,
    )
}
fn next_provider(&self) -> RootProvider<Http<Client>> {
    let idx = self.provider_index.fetch_add(1, Ordering::Relaxed) % self.read_providers.len();
    self.read_providers[idx].clone()
}
    /// تحديث مفصل لمجموعة مستخدمين باستخدام Multicall3
    pub async fn batch_update_users_detailed(&self, users: &[Address]) -> Result<()> {
        if users.is_empty() { return Ok(()); }
        let _permit = self.semaphore.acquire().await?;

      let provider = self.next_provider();
let multicall = Multicall3::new(MULTICALL3, provider.clone());
let aave_pool = IAavePool::new(AAVE_POOL, provider.clone());
let data_provider = IAaveProtocolDataProvider::new(AAVE_DATA_PROVIDER, provider);
        // المرحلة الأولى: بيانات الحساب الشاملة
        let mut pool_calls = Vec::new();
        for user in users {
            pool_calls.push(Multicall3::Call3 {
                target: AAVE_POOL,
                allowFailure: true,
                callData: Bytes::from(aave_pool.getUserAccountData(*user).calldata().to_vec()),
            });
        }
        let pool_results = multicall.aggregate3(pool_calls).call().await?;

        // تجميع الأصول لفحص التفاصيل
        let all_assets: Vec<Address> = SUPPORTED_COLLATERAL.iter().map(|c| c.0)
            .chain(SUPPORTED_DEBT.iter().map(|d| d.0))
            .collect();
        let mut asset_calls = Vec::new();
        let mut user_asset_map: HashMap<usize, Vec<usize>> = HashMap::new(); // user_index -> asset call indices
        for (i, res) in pool_results.returnData.iter().enumerate() {
            if !res.success { continue; }
            let base_idx = asset_calls.len();
            for asset in &all_assets {
                asset_calls.push(Multicall3::Call3 {
                    target: AAVE_DATA_PROVIDER,
                    allowFailure: true,
                    callData: Bytes::from(data_provider.getUserReserveData(*asset, users[i]).calldata().to_vec()),
                });
            }
            user_asset_map.insert(i, (base_idx..base_idx + all_assets.len()).collect());
        }
        let asset_results = if !asset_calls.is_empty() {
            multicall.aggregate3(asset_calls).call().await?.returnData
        } else {
            vec![]
        };

        // معالجة النتائج وتحديث الكاش
        let prices = self.price_manager.get_all_prices();
        for (i, pool_res) in pool_results.returnData.iter().enumerate() {
            let user = users[i];
            if !pool_res.success { continue; }
            let pool_data = match IAavePool::getUserAccountDataCall::abi_decode_returns(&pool_res.returnData, false) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let total_debt = u256_to_f64(pool_data.totalDebtBase, 8);
            if total_debt < 10.0 { continue; }
            let health_factor = u256_to_f64(pool_data.healthFactor, 18);
            let total_collateral = u256_to_f64(pool_data.totalCollateralBase, 8);
            let liquidation_threshold = u256_to_f64(pool_data.currentLiquidationThreshold, 4);

            let mut debt_assets = HashMap::new();
            let mut collateral_assets = HashMap::new();
            if let Some(indices) = user_asset_map.get(&i) {
                for &idx in indices {
                    if idx >= asset_results.len() { continue; }
                    let asset_res = &asset_results[idx];
                    if !asset_res.success { continue; }
                    if let Ok(reserve_data) = IAaveProtocolDataProvider::getUserReserveDataCall::abi_decode_returns(&asset_res.returnData, false) {
                        let total_debt_token = reserve_data.currentStableDebt + reserve_data.currentVariableDebt;
                        if !total_debt_token.is_zero() {
                            debt_assets.insert(all_assets[idx - indices[0]], total_debt_token);
                        }
                        if reserve_data.usageAsCollateralEnabled && !reserve_data.currentATokenBalance.is_zero() {
                            collateral_assets.insert(all_assets[idx - indices[0]], reserve_data.currentATokenBalance);
                        }
                    }
                }
            }

            // تسجيل الأصول المتأثرة
            for asset in debt_assets.keys().chain(collateral_assets.keys()) {
                self.price_manager.register_user_asset(user, *asset);
            }

            let mut score = UserScore {
                address: user,
                health_factor,
                total_debt_usd: total_debt,
                total_collateral_usd: total_collateral,
                liquidation_threshold,
                liquidation_bonus: 0.05,
                priority: Priority::Watching,
                score: 0.0,
                last_update: Instant::now(),
                debt_assets,
                collateral_assets,
                liquidation_profit_estimate: 0.0,
            };
            self.cache.update_user(score);
        }
        Ok(())
    }

    /// تحديث فوري للمستخدمين المتأثرين بتغير سعر أصل معين
    async fn trigger_price_update(&self, asset: Address) {
        let affected = self.price_manager.get_affected_users(asset);
        if !affected.is_empty() {
            info!("⚡ تحديث فوري لـ {} مستخدم تأثروا بـ {:?}", affected.len(), asset);
            let _ = self.batch_update_users_detailed(&affected).await;
        }
    }

    pub async fn start(self: Arc<Self>, mut update_rx: mpsc::UnboundedReceiver<Address>) {
        let self_clone = self.clone();
        // مستمع تغيرات الأسعار
        let mut price_rx = self.price_manager.subscribe_prices();
        tokio::spawn(async move {
            while let Ok(update) = price_rx.recv().await {
                self_clone.trigger_price_update(update.asset).await;
            }
        });

        let self_clone = self.clone();
        // معالج قائمة التحديثات العادية
        tokio::spawn(async move {
            let mut queue = Vec::new();
            let mut interval = time::interval(Duration::from_millis(UPDATE_BATCH_INTERVAL_MS));
            loop {
                tokio::select! {
                    Some(user) = update_rx.recv() => {
                        queue.push(user);
                        if queue.len() >= BATCH_SIZE {
                            let batch = std::mem::take(&mut queue);
                            let _ = self_clone.batch_update_users_detailed(&batch).await;
                        }
                    }
                    _ = interval.tick() => {
                        if !queue.is_empty() {
                            let batch = std::mem::take(&mut queue);
                            let _ = self_clone.batch_update_users_detailed(&batch).await;
                        }
                    }
                }
            }
        });

        // حلقة التصفية المستمرة
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                let targets = self_clone.cache.get_top_targets(3);
                for target in targets {
                    let liq = self_clone.liquidator.clone();
                    tokio::spawn(async move {
                        let _ = liq.liquidate(&target).await;
                    });
                }
            }
        });
    }
}

// ============================================================================
// WebSocket Manager
// ============================================================================
pub struct WebSocketManager {
    price_manager: Arc<PriceManager>,
    scanner: Arc<ReactiveScanner>,
    cache: SmartCache,
    ws_urls: Vec<String>,
}

impl WebSocketManager {
    pub fn new(price_manager: Arc<PriceManager>, scanner: Arc<ReactiveScanner>, cache: SmartCache, ws_urls: Vec<String>) -> Self {
        Self { price_manager, scanner, cache, ws_urls }
    }

    pub async fn start(&self) -> Result<()> {
        self.start_price_stream().await?;
        self.start_aave_stream().await?;
        Ok(())
    }

    async fn start_price_stream(&self) -> Result<()> {
        let pm = self.price_manager.clone();
        let ws_urls = self.ws_urls.clone();
        tokio::spawn(async move {
            let feeds: Vec<Address> = SUPPORTED_COLLATERAL.iter().map(|c| c.1)
                .chain(SUPPORTED_DEBT.iter().map(|d| d.1))
                .collect();
            loop {
                for url in &ws_urls {
                    let ws = WsConnect::new(url.clone());
                    if let Ok(provider) = ProviderBuilder::new().on_ws(ws).await {
                        let filter = Filter::new().address(feeds.clone()).event_signature(ANSWER_UPDATED_SIG);
                        if let Ok(sub) = provider.subscribe_logs(&filter).await {
                            let mut stream = sub.into_stream();
                            info!("📡 WebSocket الأسعار متصل: {}", url);
                            while let Some(log) = stream.next().await {
                                if let Some((asset, price)) = decode_price_log(&log) {
                                    pm.update_price(asset, price, PriceSource::WebSocket);
                                }
                            }
                        }
                    }
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        Ok(())
    }

    async fn start_aave_stream(&self) -> Result<()> {
        let scanner = self.scanner.clone();
        let cache = self.cache.clone();
        let ws_urls = self.ws_urls.clone();
        tokio::spawn(async move {
            let sigs = vec![BORROW_EVENT_SIG, REPAY_EVENT_SIG, LIQUIDATION_EVENT_SIG];
            loop {
                for url in &ws_urls {
                    let ws = WsConnect::new(url.clone());
                    if let Ok(provider) = ProviderBuilder::new().on_ws(ws).await {
                        let filter = Filter::new().address(AAVE_POOL).event_signature(sigs.clone());
                        if let Ok(sub) = provider.subscribe_logs(&filter).await {
                            let mut stream = sub.into_stream();
                            info!("📡 WebSocket Aave متصل: {}", url);
                            while let Some(log) = stream.next().await {
                                let sig = log.topics().first().cloned().unwrap_or_default();
                                if let Some(user) = extract_user(&log) {
                                    if sig == BORROW_EVENT_SIG || sig == REPAY_EVENT_SIG {
                                        let _ = scanner.update_queue.send(user);
                                        let _ = append_to_csv(CSV_FILE_PATH, user);
                                    } else if sig == LIQUIDATION_EVENT_SIG {
                                        cache.mark_liquidated(user);
                                    }
                                }
                            }
                        }
                    }
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        Ok(())
    }
}

fn decode_price_log(log: &Log) -> Option<(Address, f64)> {
    let feed = log.address();
    // محاولة مطابقة الأصل مع قوائم الضمان أو الدين
    let asset = SUPPORTED_COLLATERAL.iter().find(|(_, f, _, _)| *f == feed).map(|c| c.0)
        .or_else(|| SUPPORTED_DEBT.iter().find(|(_, f, _)| *f == feed).map(|d| d.0))?;

    // ✅ حذف سطر DAI = 1.0 الثابت
    if log.topics().len() < 3 { return None; }

    let current_bytes: [u8; 32] = log.topics()[1].0;
    let price_i256 = I256::from_be_bytes(current_bytes);
    let price = i256_to_f64(price_i256, 8);

    if price <= 0.0 || price > 1_000_000.0 { return None; }
    Some((asset, price))
}
fn extract_user(log: &Log) -> Option<Address> {
    log.topics().get(1).map(|t| Address::from_slice(&t.0[12..32]))
}

fn load_csv(path: &str) -> Vec<Address> {
    if !Path::new(path).exists() {
        return vec![];
    }
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            warn!("⚠️ لا يمكن فتح ملف CSV: {} - {}", path, e);
            return vec![];
        }
    };
    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let trimmed = l.trim();
            match Address::from_str(trimmed) {
                Ok(addr) => Some(addr),
                Err(e) => {
                    warn!("⚠️ تجاهل عنوان غير صالح في CSV: '{}' - {}", trimmed, e);
                    None
                }
            }
        })
        .collect()
}
fn append_to_csv(path: &str, addr: Address) -> Result<()> {
    let existing = load_csv(path);
    if existing.contains(&addr) { return Ok(()); }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{:?}", addr)?;
    Ok(())
}

// تحميل CSV باستخدام Multicall3 (سريع جداً)
async fn load_csv_users_multicall(
    provider: Arc<RootProvider<Http<Client>>>,
    cache: &SmartCache,
    csv_path: &str,
) -> Result<usize> {
    let addresses = load_csv(csv_path);
    if addresses.is_empty() { return Ok(0); }
    info!("📋 تحميل {} عنوان من CSV باستخدام Multicall3...", addresses.len());

    let multicall = Multicall3::new(MULTICALL3, provider.clone());
    let aave_pool = IAavePool::new(AAVE_POOL, provider.clone());
    let mut loaded = 0;

    for chunk in addresses.chunks(BATCH_SIZE) {
    let calls: Vec<_> = chunk.iter().map(|user| {
        Multicall3::Call3 {
            target: AAVE_POOL,
            allowFailure: true,
            callData: Bytes::from(aave_pool.getUserAccountData(*user).calldata().to_vec()),
        }
    }).collect();

    // تغليف استدعاء Multicall بمعالجة الأخطاء
    let results = match multicall.aggregate3(calls.clone()).call().await {
        Ok(r) => r,
        Err(e) => {
            warn!("⚠️ فشل استدعاء Multicall3 للدفعة ({} مستخدم): {:?}", chunk.len(), e);
            // محاولة فردية لكل مستخدم في الدفعة كخطة احتياطية
            for (i, user) in chunk.iter().enumerate() {
                match aave_pool.getUserAccountData(*user).call().await {
                    Ok(data) => {
                        let total_debt = u256_to_f64(data.totalDebtBase, 8);
                        if total_debt >= 10.0 {
                            let mut score = UserScore {
                                address: *user,
                                health_factor: u256_to_f64(data.healthFactor, 18),
                                total_debt_usd: total_debt,
                                total_collateral_usd: u256_to_f64(data.totalCollateralBase, 8),
                                liquidation_threshold: u256_to_f64(data.currentLiquidationThreshold, 4),
                                liquidation_bonus: 0.05,
                                priority: Priority::Watching,
                                score: 0.0,
                                last_update: Instant::now(),
                                debt_assets: HashMap::new(),
                                collateral_assets: HashMap::new(),
                                liquidation_profit_estimate: 0.0,
                            };
                            cache.update_user(score);
                            loaded += 1;
                        }
                    }
                    Err(_) => continue,
                }
            }
            continue; // انتقل للدفعة التالية
        }
    };

    // معالجة النتائج الناجحة (الكود الأصلي)
    for (i, res) in results.returnData.iter().enumerate() {
        if !res.success { continue; }
        let decoded = match IAavePool::getUserAccountDataCall::abi_decode_returns(&res.returnData, false) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let total_debt = u256_to_f64(decoded.totalDebtBase, 8);
        if total_debt < 10.0 { continue; }
        let mut score = UserScore {
            address: chunk[i],
            health_factor: u256_to_f64(decoded.healthFactor, 18),
            total_debt_usd: total_debt,
            total_collateral_usd: u256_to_f64(decoded.totalCollateralBase, 8),
            liquidation_threshold: u256_to_f64(decoded.currentLiquidationThreshold, 4),
            liquidation_bonus: 0.05,
            priority: Priority::Watching,
            score: 0.0,
            last_update: Instant::now(),
            debt_assets: HashMap::new(),
            collateral_assets: HashMap::new(),
            liquidation_profit_estimate: 0.0,
        };
        cache.update_user(score);
        loaded += 1;
    }
    info!("📊 تمت معالجة دفعة من {} مستخدم، الإجمالي: {}", chunk.len(), loaded);
}
    info!("📂 تم تحميل {} مستخدم صالح من CSV", loaded);
    Ok(loaded)
}

// ============================================================================
// Main
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    dotenv().ok();

    info!("💀 GRIM REAPER X - ULTIMATE FUSION 💀");

    let private_key = env::var("PRIVATE_KEY").expect("PRIVATE_KEY غير موجود");
    let signer: PrivateKeySigner = private_key.parse()?;
    let contract_addr: Address = env::var("GRIM_REAPER_ADDRESS")?.parse()?;
    let telegram_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
    let telegram_chat = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();

    // جمع نقاط الاتصال
    let mut rpc_urls = vec![];
    let mut ws_urls = vec![];
    for (key, val) in env::vars() {
        if (key.starts_with("POLYGRPC_URL") || key.starts_with("POLYGON_RPC_URL")) && !val.is_empty() {
            rpc_urls.push(val.clone());
        }
        if key.starts_with("POLYGON_WS_URL") && !val.is_empty() {
            ws_urls.push(val.clone());
        }
    }
    if rpc_urls.is_empty() {
        rpc_urls = vec![
            "https://poly.api.pocket.network".into(),
            "https://polygon.drpc.org".into(),
            "https://1rpc.io/matic".into(),
        ];
    }
    if ws_urls.is_empty() {
        ws_urls = vec![
            "wss://polygon.drpc.org".into(),
            "wss://polygon-bor-rpc.publicnode.com".into(),
        ];
    }

    let read_provider = Arc::new(ProviderBuilder::new().on_http(rpc_urls[0].parse()?));
    let write_rpc = env::var("EXECUTION_RPC_URL").unwrap_or(rpc_urls[0].clone());
    let write_provider = ProviderBuilder::new().on_http(write_rpc.parse()?);

    // مدير الأسعار
    let price_manager = Arc::new(PriceManager::new());

    // تحميل الأسعار الأولية
    info!("📊 جلب الأسعار الأولية...");
    for (asset, feed, decimals, _) in SUPPORTED_COLLATERAL {
        let contract = IAggregatorV3::new(*feed, read_provider.clone());
        if let Ok(round) = contract.latestRoundData().call().await {
            let price = i256_to_f64(round.answer, *decimals);
            price_manager.update_price(*asset, price, PriceSource::RPC);
            info!("💰 {:?}: ${:.2}", asset, price);
        }
    }
    for (asset, feed, decimals) in SUPPORTED_DEBT {
        if *asset == DAI {
            price_manager.update_price(*asset, 1.0, PriceSource::RPC);
            continue;
        }
        let contract = IAggregatorV3::new(*feed, read_provider.clone());
        if let Ok(round) = contract.latestRoundData().call().await {
            let price = i256_to_f64(round.answer, *decimals);
            price_manager.update_price(*asset, price, PriceSource::RPC);
            info!("💰 {:?}: ${:.2}", asset, price);
        }
    }

    let cache = SmartCache::new();
let write_provider_clone = write_provider.clone();
    let liquidator = Arc::new(AdvancedLiquidator::new(
        read_provider.clone(),
        write_provider,
        contract_addr,
        signer,
        cache.clone(),
        price_manager.clone(),
        telegram_token,
        telegram_chat,
        &rpc_urls,
    ));
let grim_contract = GrimReaperV6::new(contract_addr, write_provider_clone);
match grim_contract.setupApprovals().send().await {
    Ok(_) => info!("✅ تم إعطاء الموافقات للعقد"),
    Err(e) => warn!("setupApprovals قد يكون نُفّذ سابقاً أو فشل: {:?}", e),
}
   let (scanner, update_rx) = ReactiveScanner::new(
    &rpc_urls,                // <-- تمرير قائمة الروابط كاملة
    cache.clone(),
    price_manager.clone(),
    liquidator.clone(),
);
    let scanner = Arc::new(scanner);

    // تحميل CSV بسرعة فائقة
    let loaded = load_csv_users_multicall(read_provider.clone(), &cache, CSV_FILE_PATH).await?;
    if loaded > 0 {
        info!("✅ تم تحميل {} مستخدم. بدء التحديث التفصيلي...", loaded);
        // إرسال جميع المستخدمين ذوي الأولوية للتحديث التفصيلي
        let critical_addr: Vec<Address> = cache.users.iter()
            .filter(|e| e.value().health_factor < 1.05)
            .map(|e| *e.key())
            .collect();
        for addr in critical_addr {
            let _ = scanner.update_queue.send(addr);
        }
        time::sleep(Duration::from_secs(3)).await;
    }

    let ws_manager = WebSocketManager::new(price_manager.clone(), scanner.clone(), cache.clone(), ws_urls);
    ws_manager.start().await?;

    scanner.start(update_rx).await;

    // مهمة تنظيف الكاش
    let cache_cleanup = cache.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(300));
        loop { interval.tick().await; cache_cleanup.cleanup(); }
    });

    // تحديث الأسعار بشكل دوري
    let pm_clone = price_manager.clone();
    let rp_clone = read_provider.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            for (asset, feed, decimals, _) in SUPPORTED_COLLATERAL {
                let contract = IAggregatorV3::new(*feed, rp_clone.clone());
                if let Ok(round) = contract.latestRoundData().call().await {
                    let price = i256_to_f64(round.answer, *decimals);
                    pm_clone.update_price(*asset, price, PriceSource::RPC);
                }
            }
            for (asset, feed, decimals) in SUPPORTED_DEBT {
                if *asset == DAI { continue; }
                let contract = IAggregatorV3::new(*feed, rp_clone.clone());
                if let Ok(round) = contract.latestRoundData().call().await {
                    let price = i256_to_f64(round.answer, *decimals);
                    pm_clone.update_price(*asset, price, PriceSource::RPC);
                }
            }
        }
    });

    // لوحة القيادة
    let cache_dash = cache.clone();
    let pm_dash = price_manager.clone();
    let liq_dash = liquidator.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let total = cache_dash.total_users();
            let critical = cache_dash.critical_count();
            let prices = pm_dash.get_all_prices();
            let (att, succ, fail) = liq_dash.get_stats();

            print!("\x1B[2J\x1B[1;1H");
            println!("╔══════════════════════════════════════════════════════════╗");
            println!("║   💀 GRIM REAPER X - ULTIMATE FUSION 💀                 ║");
            println!("╠══════════════════════════════════════════════════════════╣");
            println!("║ 👥 المراقبون: {:6} | 🚨 حرج: {:3}                       ║", total, critical);
            println!("║ 🎯 محاولات: {} | ✅ نجاح: {} | ❌ فشل: {}              ║", att, succ, fail);
            println!("╠══════════════════════════════════════════════════════════╣");
            println!("║ 💹 الأسعار:                                              ║");
            for (asset, price) in prices.iter() {
                let vol = pm_dash.get_volatility(*asset);
                println!("║   {:?}: ${:.4} (تقلب: {:.2}%)            ║", asset, price, vol * 100.0);
            }
            println!("╚══════════════════════════════════════════════════════════╝");
            if critical > 0 {
                println!("\n🚨 أهداف التصفية:");
                for (i, s) in cache_dash.get_top_targets(5).iter().enumerate() {
                    println!(" {}. {:?} | HF: {:.4} | دين: ${:.0} | ربح مقدر: ${:.2}",
                             i+1, s.address, s.health_factor, s.total_debt_usd, s.liquidation_profit_estimate);
                }
            }
        }
    });

    info!("🚀 البوت يعمل الآن بأقصى إمكانياته!");
    loop { time::sleep(Duration::from_secs(3600)).await; }
}