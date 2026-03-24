use log::debug;
use solana_entry::entry::Entry;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use std::sync::LazyLock;

const PUMP_BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMP_SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMP_EXACT_IN_DISCRIMINATOR: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];

const PUMPFUN_PROGRAM_ID_STR: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const AXIOM_PROGRAM_ID_STR: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";

static PUMPFUN_PROGRAM_ID: LazyLock<Pubkey> =
    LazyLock::new(|| PUMPFUN_PROGRAM_ID_STR.parse().unwrap());
static AXIOM_PROGRAM_ID: LazyLock<Pubkey> = LazyLock::new(|| AXIOM_PROGRAM_ID_STR.parse().unwrap());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeType {
    Unknown = 0,
    PumpfunBuy = 1,
    PumpfunSell = 2,
    PumpfunBuyExactIn = 3,
    AxiomBuy = 4,
    AxiomSell = 5,
}

impl From<TradeType> for i32 {
    fn from(t: TradeType) -> Self {
        t as i32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Unspecified = 0,
    Pumpfun = 1,
    Axiom = 2,
}

impl From<Origin> for i32 {
    fn from(o: Origin) -> Self {
        o as i32
    }
}

#[derive(Debug, Clone)]
pub struct ParsedTransaction {
    pub slot: u64,
    pub signature: String,
    pub mint: String,
    pub signer: String,
    pub trade_type: TradeType,
    pub origin: Origin,
    pub token_amount: u64,
    pub sol_amount: u64,
    pub timestamp: u64,
}

pub struct PumpFunParser;

impl PumpFunParser {
    pub fn parse_entries(entries_bytes: &[u8], filter: &str) -> Vec<ParsedTransaction> {
        let entries: Vec<Entry> = match bincode::deserialize(entries_bytes) {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to deserialize entries: {}", e);
                return vec![];
            }
        };

        let mut results = vec![];

        for entry in entries {
            for tx in entry.transactions {
                if let Some(parsed) = Self::parse_transaction(&tx, filter) {
                    results.push(parsed);
                }
            }
        }

        results
    }

    fn parse_transaction(
        transaction: &VersionedTransaction,
        filter: &str,
    ) -> Option<ParsedTransaction> {
        let account_keys = transaction.message.static_account_keys();
        let signature = transaction.signatures.get(0)?.to_string();

        for instruction in transaction.message.instructions() {
            if instruction.data.len() < 8 {
                continue;
            }

            let program_id = account_keys.get(instruction.program_id_index as usize)?;
            let data = &instruction.data;

            if *program_id == *PUMPFUN_PROGRAM_ID {
                if filter == "axiom" {
                    continue;
                }

                let discriminator = &data[..8];
                if let Some((trade_type, token_amount, sol_amount)) =
                    Self::parse_pumpfun_args(discriminator, data)
                {
                    let signer = account_keys
                        .get(0)
                        .map(|k| k.to_string())
                        .unwrap_or_default();
                    let mint = Self::resolve_mint(account_keys, instruction, 2).unwrap_or_default();

                    return Some(ParsedTransaction {
                        slot: 0,
                        signature,
                        mint,
                        signer,
                        trade_type,
                        origin: Origin::Pumpfun,
                        token_amount,
                        sol_amount,
                        timestamp: 0,
                    });
                }
            }

            if *program_id == *AXIOM_PROGRAM_ID {
                if filter == "pumpfun" {
                    continue;
                }

                if data.len() == 22 && data[0] == 0x00 && (data[17] == 0x00 || data[17] == 0x01) {
                    let is_sell = data[17] == 0x01;
                    let amount_in = u64::from_le_bytes(data[1..9].try_into().unwrap());
                    let min_amount_out = u64::from_le_bytes(data[9..17].try_into().unwrap());

                    let signer = account_keys
                        .get(0)
                        .map(|k| k.to_string())
                        .unwrap_or_default();
                    let mint =
                        Self::resolve_mint(account_keys, instruction, 10).unwrap_or_default();

                    let trade_type = if is_sell {
                        TradeType::AxiomSell
                    } else {
                        TradeType::AxiomBuy
                    };

                    let (token_amount, sol_amount) = if is_sell {
                        (amount_in, min_amount_out)
                    } else {
                        (min_amount_out, amount_in)
                    };

                    return Some(ParsedTransaction {
                        slot: 0,
                        signature,
                        mint,
                        signer,
                        trade_type,
                        origin: Origin::Axiom,
                        token_amount,
                        sol_amount,
                        timestamp: 0,
                    });
                }
            }
        }

        None
    }

    fn parse_pumpfun_args(discriminator: &[u8], data: &[u8]) -> Option<(TradeType, u64, u64)> {
        if data.len() < 24 {
            return None;
        }

        if discriminator == PUMP_BUY_DISCRIMINATOR {
            let token_amount = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let sol_amount = u64::from_le_bytes(data[16..24].try_into().unwrap());
            return Some((TradeType::PumpfunBuy, token_amount, sol_amount));
        }

        if discriminator == PUMP_SELL_DISCRIMINATOR {
            let token_amount = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let sol_amount = u64::from_le_bytes(data[16..24].try_into().unwrap());
            return Some((TradeType::PumpfunSell, token_amount, sol_amount));
        }

        if discriminator == PUMP_EXACT_IN_DISCRIMINATOR {
            let sol_amount = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let token_amount = u64::from_le_bytes(data[16..24].try_into().unwrap());
            return Some((TradeType::PumpfunBuyExactIn, token_amount, sol_amount));
        }

        None
    }

    fn resolve_mint(
        account_keys: &[Pubkey],
        instruction: &solana_sdk::instruction::CompiledInstruction,
        mint_index: usize,
    ) -> Option<String> {
        if instruction.accounts.len() > mint_index {
            let key_idx = instruction.accounts[mint_index] as usize;
            if key_idx < account_keys.len() {
                return Some(account_keys[key_idx].to_string());
            }
        }
        None
    }
}
