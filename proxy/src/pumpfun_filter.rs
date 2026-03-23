use log::debug;
use solana_entry::entry::Entry;
use solana_sdk::transaction::VersionedTransaction;

const PUMP_BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMP_SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMP_EXACT_IN_DISCRIMINATOR: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
const PUMP_CREATE_V2_DISCRIMINATOR: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];
const PUMP_CREATE_DISCRIMINATOR: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];

const AXIOM_PROGRAM_ID: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";

pub struct PumpFunFilter;

impl PumpFunFilter {
    pub fn filter_entries(entries_bytes: &[u8]) -> Option<Vec<u8>> {
        let entries: Vec<Entry> = match bincode::deserialize(entries_bytes) {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to deserialize entries: {}", e);
                return None;
            }
        };

        let filtered_entries: Vec<Entry> = entries
            .into_iter()
            .map(|mut entry| {
                entry.transactions.retain(|tx| {
                    Self::contains_pumpfun_instruction(tx) || Self::contains_axiom_instruction(tx)
                });
                entry
            })
            .filter(|entry| !entry.transactions.is_empty())
            .collect();

        if filtered_entries.is_empty() {
            return None;
        }

        match bincode::serialize(&filtered_entries) {
            Ok(filtered_bytes) => Some(filtered_bytes),
            Err(e) => {
                debug!("Failed to serialize filtered entries: {}", e);
                None
            }
        }
    }

    fn contains_pumpfun_instruction(transaction: &VersionedTransaction) -> bool {
        for instruction in transaction.message.instructions() {
            if instruction.data.len() < 8 {
                continue;
            }

            let discriminator = &instruction.data[..8];
            if Self::is_pumpfun_discriminator(discriminator) {
                return true;
            }
        }

        false
    }

    fn contains_axiom_instruction(transaction: &VersionedTransaction) -> bool {
        let account_keys = transaction.message.static_account_keys();

        for instruction in transaction.message.instructions() {
            let program_id =
                if let Some(key) = account_keys.get(instruction.program_id_index as usize) {
                    key
                } else {
                    continue;
                };

            if program_id.to_string() != AXIOM_PROGRAM_ID {
                continue;
            }

            let data = &instruction.data;
            if data.len() != 22 {
                continue;
            }

            if data[0] != 0x00 {
                continue;
            }

            if data[17] != 0x00 && data[17] != 0x01 {
                continue;
            }

            return true;
        }

        false
    }

    fn is_pumpfun_discriminator(discriminator: &[u8]) -> bool {
        discriminator == PUMP_BUY_DISCRIMINATOR
            || discriminator == PUMP_SELL_DISCRIMINATOR
            || discriminator == PUMP_EXACT_IN_DISCRIMINATOR
            || discriminator == PUMP_CREATE_V2_DISCRIMINATOR
            || discriminator == PUMP_CREATE_DISCRIMINATOR
    }
}
