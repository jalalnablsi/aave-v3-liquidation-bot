# Aave V3 Liquidation Bot

A real-time liquidation monitoring bot for the Aave V3 protocol, built in Rust. It listens to on-chain events, calculates position health factors, and triggers alerts when positions become undercollateralized.

---

## Prerequisites

- **Rust** (1.70 or higher) → [Install Rust](https://www.rust-lang.org/tools/install)
- **Cargo** (comes with Rust)
- An **RPC endpoint** (Alchemy, Infura, or your own node)
- A wallet **private key** for the bot (use a dedicated bot wallet, never your personal one)

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/aave-v3-liquidation-bot.git
cd aave-v3-liquidation-bot

### 2. Create your `.env` file

Copy the example file and fill in your own values:

cp .env.example .env
### Build

cargo build --release
### RUN
./target/release/grim-reaper-x
