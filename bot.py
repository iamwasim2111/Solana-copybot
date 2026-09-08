"""
Solana Copy-Trading Telegram Bot
---------------------------------
Watches a target wallet for swaps (buys/sells) on Solana and mirrors
those trades from the user's own wallet via the Jupiter Aggregator.

IMPORTANT SAFETY NOTES (read before running with real funds):
  - This is a starting skeleton, not a finished, audited trading system.
  - Test with a very small amount of SOL before trusting it with real capital.
  - Network issues, RPC downtime, or fast price moves can cause missed or
    failed trades. There is no guarantee this will execute in time.
  - Never share your private key with anyone. It is loaded only from the
    local .env / Railway environment variables, never hard-coded.
"""

import os
import json
import asyncio
import logging
import base64
from datetime import datetime

import requests
import websockets
from dotenv import load_dotenv

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("copybot")

HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "")
HELIUS_WS_URL = HELIUS_RPC_URL.replace("https://", "wss://") if HELIUS_RPC_URL else ""
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TARGET_WALLET = os.getenv("TARGET_WALLET", "")
MY_PRIVATE_KEY = os.getenv("MY_PRIVATE_KEY", "")  # base58 string, kept secret

JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL = "https://quote-api.jup.ag/v6/swap"
SOL_MINT = "So11111111111111111111111111111111111111112"

# Runtime state (kept in memory; resets if the bot restarts)
state = {
    "tracking": bool(TARGET_WALLET),
    "buy_amount_sol": 0.01,   # default per-trade amount in SOL
    "slippage_bps": 100,      # 1% default slippage
    "chat_id": None,          # where to send notifications
    "trade_history": [],
}

keypair = None
if MY_PRIVATE_KEY:
    try:
        keypair = Keypair.from_base58_string(MY_PRIVATE_KEY)
    except Exception as e:
        log.error("Could not load private key: %s", e)


# ---------------------------------------------------------------------------
# Jupiter swap execution
# ---------------------------------------------------------------------------

async def get_jupiter_quote(input_mint: str, output_mint: str, amount: int, slippage_bps: int):
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": slippage_bps,
    }
    resp = requests.get(JUPITER_QUOTE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


async def execute_swap(input_mint: str, output_mint: str, amount: int, slippage_bps: int, client: AsyncClient):
    """Get a quote from Jupiter, build the swap transaction, sign it with our
    keypair, and send it to the network. Returns the transaction signature."""
    if keypair is None:
        raise RuntimeError("No private key loaded — cannot execute trades yet.")

    quote = await get_jupiter_quote(input_mint, output_mint, amount, slippage_bps)

    swap_resp = requests.post(
        JUPITER_SWAP_URL,
        json={
            "quoteResponse": quote,
            "userPublicKey": str(keypair.pubkey()),
            "wrapAndUnwrapSol": True,
            "prioritizationFeeLamports": "auto",
        },
        timeout=15,
    )
    swap_resp.raise_for_status()
    swap_tx_b64 = swap_resp.json()["swapTransaction"]

    raw_tx = VersionedTransaction.from_bytes(base64.b64decode(swap_tx_b64))
    signed_tx = VersionedTransaction(raw_tx.message, [keypair])

    result = await client.send_raw_transaction(
        bytes(signed_tx),
        opts={"skip_preflight": False, "preflight_commitment": Confirmed},
    )
    return str(result.value)


# ---------------------------------------------------------------------------
# Target wallet monitoring
# ---------------------------------------------------------------------------

async def parse_transaction_for_swap(client: AsyncClient, signature: str):
    """Fetch a transaction and figure out, in a basic way, whether the
    target wallet bought or sold a token, and which mint / amount.

    NOTE: robust swap parsing across every DEX program is nontrivial.
    This checks token balance changes for the target wallet as a
    reasonably reliable general-purpose signal.
    """
    tx = await client.get_transaction(
        signature,
        max_supported_transaction_version=0,
    )
    if tx.value is None:
        return None

    meta = tx.value.transaction.meta
    if meta is None or meta.err is not None:
        return None  # failed transaction, ignore

    pre_balances = {b.mint: b for b in meta.pre_token_balances}
    post_balances = {b.mint: b for b in meta.post_token_balances}

    changes = []
    for mint, post in post_balances.items():
        pre_amount = float(pre_balances[mint].ui_token_amount.ui_amount_string) if mint in pre_balances and pre_balances[mint].ui_token_amount.ui_amount_string else 0.0
        post_amount = float(post.ui_token_amount.ui_amount_string) if post.ui_token_amount.ui_amount_string else 0.0
        delta = post_amount - pre_amount
        if abs(delta) > 0:
            changes.append((mint, delta))

    if not changes:
        return None

    # Heuristic: the token whose balance increased is what was bought;
    # if a balance decreased instead, treat it as a sell of that token.
    changes.sort(key=lambda c: abs(c[1]), reverse=True)
    mint, delta = changes[0]

    return {
        "mint": mint,
        "side": "buy" if delta > 0 else "sell",
        "amount_change": abs(delta),
        "signature": signature,
    }


async def watch_target_wallet(app: Application):
    if not HELIUS_WS_URL or not TARGET_WALLET:
        log.warning("HELIUS_RPC_URL or TARGET_WALLET not set — monitoring disabled.")
        return

    client = AsyncClient(HELIUS_RPC_URL)

    while True:
        try:
            async with websockets.connect(HELIUS_WS_URL) as ws:
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [TARGET_WALLET]},
                        {"commitment": "confirmed"},
                    ],
                }
                await ws.send(json.dumps(subscribe_msg))
                log.info("Subscribed to logs for %s", TARGET_WALLET)

                async for message in ws:
                    if not state["tracking"]:
                        continue
                    data = json.loads(message)
                    try:
                        signature = data["params"]["result"]["value"]["signature"]
                    except (KeyError, TypeError):
                        continue

                    log.info("New tx from target wallet: %s", signature)
                    await asyncio.sleep(2)  # give the RPC a moment to index it

                    swap_info = await parse_transaction_for_swap(client, signature)
                    if swap_info is None:
                        continue

                    await handle_copy_trade(app, client, swap_info)

        except Exception as e:
            log.error("Websocket error, reconnecting in 5s: %s", e)
            await asyncio.sleep(5)


async def handle_copy_trade(app: Application, client: AsyncClient, swap_info: dict):
    mint = swap_info["mint"]
    side = swap_info["side"]

    try:
        if side == "buy":
            amount_lamports = int(state["buy_amount_sol"] * 1_000_000_000)
            sig = await execute_swap(SOL_MINT, mint, amount_lamports, state["slippage_bps"], client)
        else:
            # Selling: for a first working version we sell the same mint
            # back to SOL. Sizing an exact proportional sell requires
            # tracking your own open position per token — a good next
            # improvement once the basic flow is confirmed working.
            log.info("Sell detected for %s — manual review recommended for now.", mint)
            sig = None

        entry = {
            "time": datetime.utcnow().isoformat(),
            "side": side,
            "mint": mint,
            "tx": sig,
        }
        state["trade_history"].append(entry)

        if state["chat_id"]:
            msg = f"🔔 Target wallet {side.upper()}: `{mint}`\n"
            msg += f"Tx: `{sig}`" if sig else "⚠️ Not auto-executed — sell logic needs your review."
            await app.bot.send_message(chat_id=state["chat_id"], text=msg, parse_mode="Markdown")

    except Exception as e:
        log.error("Failed to copy trade: %s", e)
        if state["chat_id"]:
            await app.bot.send_message(chat_id=state["chat_id"], text=f"❌ Copy trade failed: {e}")


# ---------------------------------------------------------------------------
# Telegram command handlers
# ---------------------------------------------------------------------------

async def cmd_track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state["chat_id"] = update.effective_chat.id
    if context.args:
        global TARGET_WALLET
        TARGET_WALLET = context.args[0]
    state["tracking"] = True
    await update.message.reply_text(f"✅ Tracking started for wallet:\n`{TARGET_WALLET}`", parse_mode="Markdown")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state["tracking"] = False
    await update.message.reply_text("⏸ Tracking stopped.")


async def cmd_setamount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setamount 0.05  (amount in SOL per trade)")
        return
    try:
        state["buy_amount_sol"] = float(context.args[0])
        await update.message.reply_text(f"✅ Buy amount set to {state['buy_amount_sol']} SOL per trade.")
    except ValueError:
        await update.message.reply_text("Please send a valid number, e.g. /setamount 0.05")


async def cmd_setslippage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setslippage 100  (in basis points, 100 = 1%)")
        return
    try:
        state["slippage_bps"] = int(context.args[0])
        await update.message.reply_text(f"✅ Slippage set to {state['slippage_bps']} bps.")
    except ValueError:
        await update.message.reply_text("Please send a valid integer, e.g. /setslippage 100")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        f"Tracking: {'ON' if state['tracking'] else 'OFF'}\n"
        f"Target wallet: `{TARGET_WALLET or 'not set'}`\n"
        f"Buy amount: {state['buy_amount_sol']} SOL\n"
        f"Slippage: {state['slippage_bps']} bps\n"
        f"Wallet loaded: {'Yes' if keypair else 'No (set MY_PRIVATE_KEY)'}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if keypair is None:
        await update.message.reply_text("No wallet loaded yet — set MY_PRIVATE_KEY first.")
        return
    client = AsyncClient(HELIUS_RPC_URL)
    resp = await client.get_balance(keypair.pubkey())
    sol_balance = resp.value / 1_000_000_000
    await update.message.reply_text(f"💰 Balance: {sol_balance:.4f} SOL")
    await client.close()


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not state["trade_history"]:
        await update.message.reply_text("No trades copied yet.")
        return
    lines = []
    for t in state["trade_history"][-10:]:
        lines.append(f"{t['time']} | {t['side'].upper()} | {t['mint'][:8]}... | tx: {t['tx']}")
    await update.message.reply_text("\n".join(lines))


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/track <wallet> - Start tracking a wallet\n"
        "/stop - Stop tracking\n"
        "/setamount <sol> - Set buy amount per trade\n"
        "/setslippage <bps> - Set slippage tolerance\n"
        "/status - Show current settings\n"
        "/balance - Show your wallet balance\n"
        "/history - Show recent copied trades\n"
        "/help - Show this message"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in environment variables.")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("track", cmd_track))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("setamount", cmd_setamount))
    app.add_handler(CommandHandler("setslippage", cmd_setslippage))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("help", cmd_help))

    async with app:
        await app.start()
        await app.updater.start_polling()
        log.info("Telegram bot started.")

        await watch_target_wallet(app)  # runs forever


if __name__ == "__main__":
    asyncio.run(main())
