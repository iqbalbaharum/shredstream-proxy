use log::{debug, info, warn};
use solana_entry::entry::Entry;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

const PUMP_BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMP_SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMP_EXACT_IN_DISCRIMINATOR: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];

const PUMPFUN_PROGRAM_ID_STR: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const AXIOM_PROGRAM_ID_STR: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";

// Known Address Lookup Table addresses for platforms
const AXIOM_ALT_STR: &str = "7RKtfATWCe98ChuwecNq8XCzAzfoK3DtZTprFsPMGtio";

static PUMPFUN_PROGRAM_ID: LazyLock<Pubkey> =
    LazyLock::new(|| PUMPFUN_PROGRAM_ID_STR.parse().unwrap());
static AXIOM_PROGRAM_ID: LazyLock<Pubkey> = LazyLock::new(|| AXIOM_PROGRAM_ID_STR.parse().unwrap());
static AXIOM_ALT: LazyLock<Pubkey> = LazyLock::new(|| AXIOM_ALT_STR.parse().unwrap());

// Cache for known lookup tables (ALT address -> list of resolved addresses)
static KNOWN_ALT_CACHE: LazyLock<Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

pub fn init_lookup_tables(rpc_url: &str) {
    info!("Initializing lookup tables with RPC: {}", rpc_url);

    let mut cache = KNOWN_ALT_CACHE.write().unwrap();

    // Fetch known ALT from RPC
    let alt_addresses = vec![*AXIOM_ALT];

    for alt in alt_addresses {
        match fetch_address_lookup_table(rpc_url, alt) {
            Ok(addresses) => {
                info!("Fetched ALT {} with {} addresses", alt, addresses.len());
                cache.insert(alt, addresses);
            }
            Err(e) => {
                warn!("Failed to fetch ALT {}: {}", alt, e);
            }
        }
    }

    info!(
        "Lookup table initialization complete, {} ALTs loaded",
        cache.len()
    );
}

#[allow(dead_code)]
fn fetch_address_lookup_table(rpc_url: &str, alt: Pubkey) -> Result<Vec<Pubkey>, String> {
    let client = reqwest::blocking::Client::new();

    let request_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            alt.to_string(),
            {
                "encoding": "base64"
            }
        ]
    });

    let response = client
        .post(rpc_url)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("HTTP error: {}", response.status()));
    }

    let json: serde_json::Value = response.json().map_err(|e| e.to_string())?;

    // Debug: log the full response
    debug!("RPC response: {:?}", json);

    if let Some(error) = json.get("error") {
        return Err(format!("RPC error: {}", error));
    }

    // Check if account exists
    if json["result"]["value"].is_null() {
        return Err("Account does not exist".to_string());
    }

    // Parse the data - can be string or array [base64, "base64"]
    let data = if let Some(data_arr) = json["result"]["value"]["data"].as_array() {
        // Format is ["base64string", "base64"]
        data_arr
            .get(0)
            .and_then(|v| v.as_str())
            .ok_or("Invalid array format")?
    } else if let Some(data_str) = json["result"]["value"]["data"].as_str() {
        data_str
    } else {
        return Err("Unknown data format".to_string());
    };

    // Decode base64
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)
        .map_err(|e| e.to_string())?;

    // Parse as AddressLookupTable (raw bytes, no zstd)
    // First 40 bytes: header (discriminator(8) + authority(32) = 40)
    // Then: num_addresses as u32, then the addresses
    if decoded.len() < 44 {
        return Err("ALT data too short".to_string());
    }

    let num_addresses = u32::from_le_bytes(decoded[40..44].try_into().unwrap()) as usize;
    let mut addresses = Vec::with_capacity(num_addresses);

    for i in 0..num_addresses {
        let offset = 44 + (i * 32);
        if offset + 32 > decoded.len() {
            break;
        }
        let pubkey_bytes: [u8; 32] = decoded[offset..offset + 32].try_into().unwrap();
        addresses.push(Pubkey::new_from_array(pubkey_bytes));
    }

    Ok(addresses)
}

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
        // Check for address table lookups - skip if unknown ALT
        if Self::has_unknown_alt(&transaction) {
            debug!("Skipping transaction with unknown address lookup table");
            return None;
        }

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
                    let signer = account_keys.get(0)?.to_string();
                    let mint = Self::resolve_mint(account_keys, instruction, 2)?;

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

                    let signer = account_keys.get(0)?.to_string();
                    let mint = Self::resolve_mint(account_keys, instruction, 10)?;

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

    fn has_unknown_alt(transaction: &VersionedTransaction) -> bool {
        // Check if transaction has address table lookups
        let address_table_lookups = match transaction.message.address_table_lookups() {
            Some(lookups) => lookups,
            None => return false, // No ALTs - safe to process
        };

        // If no ALTs, safe to process
        if address_table_lookups.is_empty() {
            return false;
        }

        // Check if all ALTs are known
        let cache = KNOWN_ALT_CACHE.read().unwrap();
        for lookup in address_table_lookups {
            if !cache.contains_key(&lookup.account_key) {
                return true; // Unknown ALT found
            }
        }

        false // All ALTs are known
    }
}
