import { ethers } from 'ethers';
import axios from 'axios';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();

const FROM_BLOCK = 38766575;
const TO_BLOCK = 38866575;

const HYPERSYNC_API_KEY = process.env.HYPERSYNC_API_KEY;
// Using chain-id 1 for Ethereum mainnet (adjust if needed)
const CHAIN_ID = 8453;
const HYPERSYNC_URL = `https://${CHAIN_ID}.hypersync.xyz/query`;

const CL_POOL = "0xA0d736dd7386230DE3Aa2e6b4F60d36a5ded2291";
// Based on the images, Transfer events are from the NFT contract
const NFT_CONTRACT = "0xA0d736dd7386230DE3Aa2e6b4F60d36a5ded2291";

// Mint event signature for v2: Mint(address indexed sender, uint256 amount0, uint256 amount1)
const MINT_EVENT_SIGNATURE = ethers.id("Mint(address,uint256,uint256)");
// Burn event signature for v2: Burn(address indexed sender, address indexed to, uint256 amount0, uint256 amount1)
const BURN_EVENT_SIGNATURE = ethers.id("Burn(address,address,uint256,uint256)");
// Transfer event signature (ERC20): Transfer(address indexed from, address indexed to, uint256 value)
const TRANSFER_EVENT_SIGNATURE = ethers.id("Transfer(address,address,uint256)");

console.log('TRANSFER_EVENT_SIGNATURE : ', TRANSFER_EVENT_SIGNATURE);

interface MintEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	sender: string;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface BurnEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	sender: string;
	to: string;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface TransferEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	from: string;
	to: string;
	value: bigint;
	timestamp: number | null;
}

interface MatchedMintEvent {
	mint: MintEvent;
	transfer: TransferEvent;
}

async function fetchLogs(
	client: string,
	contractAddress: string,
	eventTopic: string,
	startBlock: number,
	endBlock: number
): Promise<Array<any>> {
	console.log(`[fetchLogs] Starting fetch for contract ${contractAddress} from block ${startBlock} to ${endBlock}`);
	let currentBlock = startBlock;
	let logs: Array<any> = [];

	const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
	const isRetryableError = (error: any): boolean => {
		const retryableCodes = ['EPIPE', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'ECONNREFUSED'];
		const retryableStatusCodes = [429, 500, 502, 503, 504];
		
		if (error.code && retryableCodes.includes(error.code)) {
			return true;
		}
		if (error.response && retryableStatusCodes.includes(error.response.status)) {
			return true;
		}
		return false;
	};

	while (currentBlock <= endBlock) {
		let retries = 0;
		const maxRetries = 5;
		let success = false;

		while (retries < maxRetries && !success) {
			try {
				console.log(`[fetchLogs] Fetching logs from block ${currentBlock}...${retries > 0 ? ` (retry ${retries}/${maxRetries})` : ''}`);
				const queryResponse = await axios.post(client, {
					from_block: currentBlock,
					to_block: endBlock,
					logs: [
						{
							address: [contractAddress],
							topics: [[eventTopic]]
						}
					],
					field_selection: {
						log: [
							'block_number',
							'log_index',
							'transaction_index',
							'transaction_hash',
							'data',
							'address',
							'topic0',
							'topic1',
							'topic2',
							'topic3'
						],
						block: ['number', 'timestamp']
					}
				},
	            {
	                headers: {
	                  Authorization: `Bearer ${HYPERSYNC_API_KEY}`,
	                },
	              });

				console.log(`[fetchLogs] API response: next_block=${queryResponse.data.next_block}, data length=${queryResponse.data.data?.length || 0}`);

				// Concatenate logs if there are any
				if (queryResponse.data.data && queryResponse.data.data.length > 0) {
					console.log(`[fetchLogs] Received ${queryResponse.data.data.length} log entries in this batch`);
					logs = logs.concat(queryResponse.data.data);
				}

				// Update currentBlock for the next iteration
				const nextBlock = queryResponse.data.next_block;
				
				// Exit if next_block is invalid or beyond end block
				if (!nextBlock || nextBlock > endBlock) {
					console.log(`[fetchLogs] Stopping fetch: next_block=${nextBlock}, currentBlock=${currentBlock}, endBlock=${endBlock}`);
					break;
				}
				
				// If next_block equals currentBlock, it means we've reached the end or no more data
				if (nextBlock === currentBlock) {
					console.log(`[fetchLogs] next_block equals currentBlock, stopping fetch`);
					break;
				}
				
				currentBlock = nextBlock;
				console.log(`[fetchLogs] Next block: ${currentBlock}, Total logs so far: ${logs.length}`);
				success = true;
			} catch (error: any) {
				if (isRetryableError(error) && retries < maxRetries - 1) {
					retries++;
					const delay = Math.min(1000 * Math.pow(2, retries - 1), 10000); // Exponential backoff, max 10s
					console.log(`[fetchLogs] Retryable error (${error.code || error.response?.status}) at block ${currentBlock}, retrying in ${delay}ms...`);
					await sleep(delay);
				} else {
					console.error(`[fetchLogs] Error fetching logs at block ${currentBlock} (attempt ${retries + 1}/${maxRetries}):`, error.code || error.message);
					if (retries >= maxRetries - 1) {
						console.error(`[fetchLogs] Max retries reached. Stopping fetch. Total logs collected so far: ${logs.length}`);
						break;
					}
					retries++;
					const delay = Math.min(1000 * Math.pow(2, retries - 1), 10000);
					await sleep(delay);
				}
			}
		}

		if (!success) {
			console.error(`[fetchLogs] Failed to fetch after ${maxRetries} attempts. Breaking loop.`);
			break;
		}
	}

	console.log(`[fetchLogs] Processing ${logs.length} log entries, mapping timestamps...`);
	const processedLogs = logs.flatMap((entry) => {
		// Create a map of block_number to timestamp
		const blockMap = new Map(
			entry.blocks.map((block: any) => [block.number, parseInt(block.timestamp, 16)])
		);

		// Map each log with the corresponding timestamp
		return entry.logs.map((log: any) => ({
			...log,
			timestamp: blockMap.get(log.block_number) || null
		}));
	});
	console.log(`[fetchLogs] Completed. Returning ${processedLogs.length} processed logs`);
	return processedLogs;
}

function decodeMintEvent(log: any): MintEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Mint(address indexed sender, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1 is indexed sender)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeMintEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			sender: decoded.args.sender.toLowerCase(),
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeMintEvent] Successfully decoded Mint event: tx=${decodedEvent.transactionHash}, sender=${decodedEvent.sender}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeMintEvent] Error decoding Mint event:", error);
		return null;
	}
}

function decodeBurnEvent(log: any): BurnEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Burn(address indexed sender, address indexed to, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1 is indexed sender, topic2 is indexed to)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeBurnEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			sender: decoded.args.sender.toLowerCase(),
			to: decoded.args.to.toLowerCase(),
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeBurnEvent] Successfully decoded Burn event: tx=${decodedEvent.transactionHash}, sender=${decodedEvent.sender}, to=${decodedEvent.to}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeBurnEvent] Error decoding Burn event:", error);
		return null;
	}
}

function decodeTransferEvent(log: any): TransferEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Transfer(address indexed from, address indexed to, uint256 value)",
		]);
		
		// Construct topics array (topic0 is event signature, topic1 is indexed from, topic2 is indexed to, value is in data)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeTransferEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			from: decoded.args.from.toLowerCase(),
			to: decoded.args.to.toLowerCase(),
			value: decoded.args.value,
			timestamp: log.timestamp,
		};
		console.log(`[decodeTransferEvent] Successfully decoded Transfer event: tx=${decodedEvent.transactionHash}, from=${decodedEvent.from}, to=${decodedEvent.to}, value=${decodedEvent.value.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeTransferEvent] Error decoding Transfer event:", error);
		return null;
	}
}

async function main() {
	console.log("[main] Starting main function");
	console.log(`[main] Configuration: FROM_BLOCK=${FROM_BLOCK}, TO_BLOCK=${TO_BLOCK}`);
	console.log(`[main] CL_POOL=${CL_POOL}, NFT_CONTRACT=${NFT_CONTRACT}`);
	
	console.log("Querying Mint events from pool contract...");
	const mintLogs = await fetchLogs(
		HYPERSYNC_URL,
		CL_POOL,
		MINT_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK
	);
	console.log(`Found ${mintLogs.length} Mint event logs`);

	console.log("Decoding Mint events...");
	const mintEvents: MintEvent[] = [];
	let decodedCount = 0;
	for (const log of mintLogs) {
		const decoded = decodeMintEvent(log);
		if (decoded) {
			mintEvents.push(decoded);
			decodedCount++;
		}
	}
	console.log(`Decoded ${decodedCount} out of ${mintLogs.length} Mint events (${mintEvents.length} total in array)`);

	console.log("Querying Transfer events from NFT contract...");
	const transferLogs = await fetchLogs(
		HYPERSYNC_URL,
		NFT_CONTRACT,
		TRANSFER_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK
	);
	console.log(`Found ${transferLogs.length} Transfer event logs`);

	console.log("Decoding Transfer events...");
	const transferEvents: TransferEvent[] = [];
	let transferDecodedCount = 0;
	for (const log of transferLogs) {
		const decoded = decodeTransferEvent(log);
		if (decoded) {
			transferEvents.push(decoded);
			transferDecodedCount++;
		}
	}
	console.log(`Decoded ${transferDecodedCount} out of ${transferLogs.length} Transfer events (${transferEvents.length} total in array)`);

	console.log("Querying Burn events from pool contract...");
	const burnLogs = await fetchLogs(
		HYPERSYNC_URL,
		CL_POOL,
		BURN_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK
	);
	console.log(`Found ${burnLogs.length} Burn event logs`);

	console.log("Decoding Burn events...");
	const burnEvents: BurnEvent[] = [];
	let burnDecodedCount = 0;
	for (const log of burnLogs) {
		const decoded = decodeBurnEvent(log);
		if (decoded) {
			burnEvents.push(decoded);
			burnDecodedCount++;
		}
	}
	console.log(`Decoded ${burnDecodedCount} out of ${burnLogs.length} Burn events (${burnEvents.length} total in array)`);

	console.log("Matching Mint events with Transfer events (same transaction hash)...");
	
	// Create a map of transfer events by transaction hash for quick lookup
	console.log(`[main] Building transfer map by transaction hash with ${transferEvents.length} transfer events...`);
	const transferMapByTx = new Map<string, TransferEvent[]>();
	for (const transfer of transferEvents) {
		const key = transfer.transactionHash.toLowerCase();
		if (!transferMapByTx.has(key)) {
			transferMapByTx.set(key, []);
		}
		transferMapByTx.get(key)!.push(transfer);
	}
	console.log(`[main] Transfer map built with ${transferMapByTx.size} unique transaction hashes`);
	
	// Log some stats
	const mintTxHashes = new Set(mintEvents.map(m => m.transactionHash.toLowerCase()));
	const transferTxHashes = new Set(transferEvents.map(t => t.transactionHash.toLowerCase()));
	const commonTxHashes = [...mintTxHashes].filter(tx => transferTxHashes.has(tx));
	console.log(`[main] Found ${commonTxHashes.length} transaction hashes that appear in both mint and transfer events`);
	if (commonTxHashes.length > 0 && commonTxHashes.length <= 10) {
		console.log(`[main] Common transaction hashes:`, commonTxHashes);
	} else if (commonTxHashes.length > 10) {
		console.log(`[main] First 10 common transaction hashes:`, commonTxHashes.slice(0, 10));
	}

	// Match Mint events with Transfer events
	// For v2 pools, minting creates LP tokens, so Transfer events with from=0x0 indicate mints
	const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
	const matchedMintEvents: MatchedMintEvent[] = [];
	let mintMatchedCount = 0;
	let mintNoMatchCount = 0;
	
	for (const mint of mintEvents) {
		const txHash = mint.transactionHash.toLowerCase();
		const matchingTransfers = transferMapByTx.get(txHash) || [];
		
		// Filter for Transfer events where from is zero address (mint)
		const mintTransfers = matchingTransfers.filter(t => t.from.toLowerCase() === ZERO_ADDRESS);
		
		if (mintTransfers.length > 0) {
			// Match with the first mint transfer in the same transaction
			matchedMintEvents.push({ mint, transfer: mintTransfers[0] });
			mintMatchedCount++;
			if (mintMatchedCount <= 10) {
				console.log(`[main] Matched mint tx=${mint.transactionHash} with transfer, sender=${mint.sender}, transfer.to=${mintTransfers[0].to}`);
			}
		} else {
			mintNoMatchCount++;
			if (mintNoMatchCount <= 10) {
				console.log(`[main] No matching transfer found for mint tx=${mint.transactionHash}, sender=${mint.sender}`);
			}
		}
	}
	
	if (mintMatchedCount > 10) {
		console.log(`[main] ... and ${mintMatchedCount - 10} more matched mint events`);
	}
	if (mintNoMatchCount > 10) {
		console.log(`[main] ... and ${mintNoMatchCount - 10} more mints with no matching transfers`);
	}

	console.log(`Matched ${matchedMintEvents.length} mint events (${mintMatchedCount} matched, ${mintNoMatchCount} unmatched)`);
	console.log(`Found ${burnEvents.length} burn events`);

	// Write to file with new format - BOTH mint and burn events
	console.log(`[main] Preparing output data for ${matchedMintEvents.length + burnEvents.length} events...`);
	console.log(`[main] Including ${matchedMintEvents.length} mint events and ${burnEvents.length} burn events`);
	const outputPath = path.join(process.cwd(), `events_${CL_POOL}.json`);
	
	// Format mint events (only matched ones, since we need transfer.to for the user)
	const mintOutputData = matchedMintEvents.map(({ mint, transfer }) => ({
		type: "mint",
		transactionHash: mint.transactionHash,
		blockNumber: mint.blockNumber,
		amount: "0", // v2 Mint doesn't have an amount field, only amount0 and amount1
		amount0: mint.amount0.toString(),
		amount1: mint.amount1.toString(),
		user: transfer.to, // For mint, user is the 'to' address from transfer
	}));

	// Format burn events (ALL burn events, using to field directly)
	const burnOutputData = burnEvents.map((burn) => ({
		type: "burn",
		transactionHash: burn.transactionHash,
		blockNumber: burn.blockNumber,
		amount: "0", // v2 Burn doesn't have a single amount field
		amount0: burn.amount0.toString(),
		amount1: burn.amount1.toString(),
		user: burn.to, // For burn, user is the 'to' address from the event
	}));

	// Combine BOTH mint and burn events, then sort by block number (or transaction hash)
	const outputData = [...mintOutputData, ...burnOutputData].sort((a, b) => {
		if (a.blockNumber !== b.blockNumber) {
			return a.blockNumber - b.blockNumber;
		}
		return a.transactionHash.localeCompare(b.transactionHash);
	});

	console.log(`[main] Writing ${outputData.length} entries to file: ${outputPath}`);
	console.log(`[main] Breakdown: ${mintOutputData.length} mint events, ${burnOutputData.length} burn events`);
	fs.writeFileSync(outputPath, JSON.stringify(outputData, null, 2));
	console.log(`Results written to ${outputPath}`);
	console.log(`Total events: ${outputData.length} (${matchedMintEvents.length} mints, ${burnEvents.length} burns)`);
	console.log("[main] Main function completed successfully");
}

console.log("[startup] Script starting...");
main().catch((error) => {
	console.error("[startup] Fatal error in main:", error);
	process.exit(1);
});
