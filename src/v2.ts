import { ethers } from 'ethers';
import axios from 'axios';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();

// Cache configuration
const CACHE_DIR = path.join(process.cwd(), 'cache');
const USE_CACHE = process.env.USE_CACHE === 'true'; // Set USE_CACHE=true to load from cache

const FROM_BLOCK = 39557809;
const TO_BLOCK = 40719263;

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

// Streaming version that processes logs without storing them all in memory
async function fetchAndProcessLogs(
	client: string,
	contractAddress: string,
	eventTopic: string,
	startBlock: number,
	endBlock: number,
	processCallback: (log: any) => void
): Promise<number> {
	console.log(`[fetchAndProcessLogs] Starting fetch for contract ${contractAddress} from block ${startBlock} to ${endBlock}`);
	let currentBlock = startBlock;
	let totalProcessed = 0;

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

				const nextBlock = queryResponse.data.next_block;
				const dataArray = queryResponse.data.data || [];
				
				console.log(`[fetchLogs] API response: next_block=${nextBlock}, data length=${dataArray.length}`);

				// Find the highest block number in this batch and process logs incrementally
				let maxBlockInBatch = currentBlock - 1;
				if (dataArray.length > 0) {
					// Process each entry immediately to avoid memory buildup
					let logCount = 0;
					for (const entry of dataArray) {
						if (!entry || !entry.logs || !Array.isArray(entry.logs)) {
							continue;
						}
						
						// Create a map of block_number to timestamp for this entry
						const blockMap = new Map(
							(entry.blocks || []).map((block: any) => [block.number, parseInt(block.timestamp, 16)])
						);

						// Process logs from this entry immediately
						for (const log of entry.logs) {
							if (log.block_number && log.block_number > maxBlockInBatch) {
								maxBlockInBatch = log.block_number;
							}
							// Process log immediately via callback instead of storing
							const processedLog = {
								...log,
								timestamp: blockMap.get(log.block_number) || null
							};
							processCallback(processedLog);
							logCount++;
							totalProcessed++;
						}
					}
					console.log(`[fetchLogs] Received ${dataArray.length} response entries with ${logCount} total logs in this batch, max block: ${maxBlockInBatch}`);
				}

				// Check if we should continue
				if (!nextBlock) {
					console.log(`[fetchLogs] No next_block returned, stopping fetch`);
					break;
				}
				
				// If next_block is beyond end block, we're done
				if (nextBlock > endBlock) {
					console.log(`[fetchLogs] next_block ${nextBlock} exceeds endBlock ${endBlock}, stopping fetch`);
					break;
				}
				
				// If next_block is less than currentBlock, something is wrong - increment to avoid infinite loop
				if (nextBlock < currentBlock) {
					console.warn(`[fetchLogs] next_block ${nextBlock} is less than currentBlock ${currentBlock}, incrementing currentBlock`);
					currentBlock = currentBlock + 1;
				}
				// If next_block equals currentBlock and we got no data, we're done
				else if (nextBlock === currentBlock && dataArray.length === 0) {
					console.log(`[fetchLogs] next_block equals currentBlock and no data returned, stopping fetch`);
					break;
				}
				// If next_block equals currentBlock but we got data, increment by 1 to avoid infinite loop
				else if (nextBlock === currentBlock) {
					console.log(`[fetchLogs] next_block equals currentBlock but we got data, incrementing to ${currentBlock + 1}`);
					currentBlock = currentBlock + 1;
				}
				// If next_block equals endBlock
				else if (nextBlock === endBlock) {
					// If we got no data, we're done
					if (dataArray.length === 0) {
						console.log(`[fetchLogs] next_block equals endBlock and no data returned, stopping fetch`);
						break;
					}
					// If we got data but maxBlockInBatch is less than endBlock, there might be more data
					else if (maxBlockInBatch < endBlock) {
						console.log(`[fetchLogs] next_block equals endBlock but maxBlockInBatch ${maxBlockInBatch} < endBlock ${endBlock}, continuing from ${maxBlockInBatch + 1}`);
						currentBlock = maxBlockInBatch + 1;
					}
					// If we got data and maxBlockInBatch equals endBlock, we're done
					else {
						console.log(`[fetchLogs] next_block equals endBlock and maxBlockInBatch ${maxBlockInBatch} equals endBlock, stopping fetch`);
						break;
					}
				} else {
					currentBlock = nextBlock;
				}
				
				console.log(`[fetchAndProcessLogs] Next block: ${currentBlock}, Total processed logs so far: ${totalProcessed}`);
				success = true;
			} catch (error: any) {
				if (isRetryableError(error) && retries < maxRetries - 1) {
					retries++;
					const delay = Math.min(1000 * Math.pow(2, retries - 1), 10000); // Exponential backoff, max 10s
					console.log(`[fetchAndProcessLogs] Retryable error (${error.code || error.response?.status}) at block ${currentBlock}, retrying in ${delay}ms...`);
					await sleep(delay);
				} else {
					console.error(`[fetchAndProcessLogs] Error fetching logs at block ${currentBlock} (attempt ${retries + 1}/${maxRetries}):`, error.code || error.message);
					if (retries >= maxRetries - 1) {
						console.error(`[fetchAndProcessLogs] Max retries reached. Stopping fetch. Total logs collected so far: ${totalProcessed}`);
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

	console.log(`[fetchAndProcessLogs] Completed. Total processed logs: ${totalProcessed}`);
	return totalProcessed;
}

// Simple CSV escaping - wrap in quotes if contains comma, quote, or newline
function escapeCsvValue(value: string): string {
	if (value.includes(',') || value.includes('"') || value.includes('\n')) {
		return '"' + value.replace(/"/g, '""') + '"';
	}
	return value;
}

// Cache helper functions
function ensureCacheDir(): void {
	if (!fs.existsSync(CACHE_DIR)) {
		fs.mkdirSync(CACHE_DIR, { recursive: true });
		console.log(`[cache] Created cache directory: ${CACHE_DIR}`);
	}
}

function getCacheFilePath(eventType: string): string {
	return path.join(CACHE_DIR, `${eventType}_${CL_POOL}.json`);
}

function saveToCache<T>(eventType: string, data: T[]): void {
	ensureCacheDir();
	const filePath = getCacheFilePath(eventType);
	// Convert BigInt to string for JSON serialization
	const jsonData = JSON.stringify(data, (key, value) =>
		typeof value === 'bigint' ? value.toString() : value
	, 2);
	fs.writeFileSync(filePath, jsonData);
	console.log(`[cache] Saved ${data.length} ${eventType} events to ${filePath}`);
}

function loadFromCache<T>(eventType: string): T[] | null {
	const filePath = getCacheFilePath(eventType);
	if (!fs.existsSync(filePath)) {
		console.log(`[cache] No cache file found for ${eventType}`);
		return null;
	}
	const jsonData = fs.readFileSync(filePath, 'utf-8');
	const data = JSON.parse(jsonData);
	console.log(`[cache] Loaded ${data.length} ${eventType} events from cache`);
	return data;
}

// Convert cached data back to proper types (BigInt fields)
function restoreBigInts<T>(obj: any, bigIntFields: string[]): T {
	const result = { ...obj };
	for (const field of bigIntFields) {
		if (result[field] !== undefined && result[field] !== null) {
			result[field] = BigInt(result[field]);
		}
	}
	return result as T;
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

	// Check if API key is set
	if (!HYPERSYNC_API_KEY) {
		console.error("[main] ERROR: HYPERSYNC_API_KEY is not set in .env file!");
		console.error("[main] Please create a .env file with: HYPERSYNC_API_KEY=your_api_key_here");
		process.exit(1);
	}

	console.log(`[main] Configuration: FROM_BLOCK=${FROM_BLOCK}, TO_BLOCK=${TO_BLOCK}`);
	console.log(`[main] CL_POOL=${CL_POOL}, NFT_CONTRACT=${NFT_CONTRACT}`);
	console.log(`[main] API Key present: ${HYPERSYNC_API_KEY.substring(0, 10)}...`);
	console.log(`[main] USE_CACHE=${USE_CACHE}`);

	const outputPath = path.join(process.cwd(), `events_${CL_POOL}.csv`);
	const outputFile = fs.createWriteStream(outputPath);

	// Handle file write errors
	outputFile.on('error', (err) => {
		console.error('[main] File write error:', err);
		process.exit(1);
	});

	// Write CSV header
	const headers = ['type', 'transactionHash', 'blockNumber', 'amount', 'amount0', 'amount1', 'user'];
	outputFile.write(headers.map(escapeCsvValue).join(',') + '\n');

	let totalMintEvents = 0;
	let totalBurnEvents = 0;
	let matchedMintCount = 0;
	let unmatchedMintCount = 0;
	let matchedBurnCount = 0;
	let unmatchedBurnCount = 0;

	// ==================== DATA LOADING PHASE ====================
	// Either fetch from HyperSync API or load from cache

	let allTransferEvents: TransferEvent[] = [];
	let allMintEvents: MintEvent[] = [];
	let allBurnEvents: BurnEvent[] = [];

	if (USE_CACHE) {
		console.log("[main] USE_CACHE=true, loading from cache...");

		// Load Transfer events
		const cachedTransfers = loadFromCache<any>('transfer');
		if (cachedTransfers) {
			allTransferEvents = cachedTransfers.map((e: any) => restoreBigInts<TransferEvent>(e, ['value']));
		}

		// Load Mint events
		const cachedMints = loadFromCache<any>('mint');
		if (cachedMints) {
			allMintEvents = cachedMints.map((e: any) => restoreBigInts<MintEvent>(e, ['amount0', 'amount1']));
		}

		// Load Burn events
		const cachedBurns = loadFromCache<any>('burn');
		if (cachedBurns) {
			allBurnEvents = cachedBurns.map((e: any) => restoreBigInts<BurnEvent>(e, ['amount0', 'amount1']));
		}

		console.log("[main] Loaded all events from cache");
	} else {
		console.log("[main] Fetching fresh data from HyperSync API...");

		// Step 1: Fetch Transfer events
		console.log("Querying Transfer events from LP token contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			NFT_CONTRACT,
			TRANSFER_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeTransferEvent(log);
				if (decoded) {
					allTransferEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allTransferEvents.length} Transfer events`);
		saveToCache('transfer', allTransferEvents);

		// Step 2: Fetch Mint events
		console.log("Querying Mint events from pool contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			CL_POOL,
			MINT_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeMintEvent(log);
				if (decoded) {
					allMintEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allMintEvents.length} Mint events`);
		saveToCache('mint', allMintEvents);

		// Step 3: Fetch Burn events
		console.log("Querying Burn events from pool contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			CL_POOL,
			BURN_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeBurnEvent(log);
				if (decoded) {
					allBurnEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allBurnEvents.length} Burn events`);
		saveToCache('burn', allBurnEvents);

		console.log("[main] All data fetched and cached");
	}

	// ==================== DATA PROCESSING PHASE ====================
	console.log("[main] Processing events...");

	const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

	// Build transfer maps from cached/fetched data
	// Store mint transfers (from=0x0): tx hash -> array of {to, value, logIndex}
	const mintTransferMapByTx = new Map<string, Array<{to: string, value: bigint, logIndex: number}>>();
	// Store burn transfers (to=0x0): tx hash -> array of {from, value, logIndex}
	const burnTransferMapByTx = new Map<string, Array<{from: string, value: bigint, logIndex: number}>>();

	for (const transfer of allTransferEvents) {
		const key = transfer.transactionHash.toLowerCase();
		// Store mint transfers (from=0x0) for matching with Mint events
		if (transfer.from.toLowerCase() === ZERO_ADDRESS) {
			if (!mintTransferMapByTx.has(key)) {
				mintTransferMapByTx.set(key, []);
			}
			mintTransferMapByTx.get(key)!.push({
				to: transfer.to,
				value: transfer.value,
				logIndex: transfer.logIndex
			});
		}
		// Store burn transfers (to=0x0) for matching with Burn events
		if (transfer.to.toLowerCase() === ZERO_ADDRESS) {
			if (!burnTransferMapByTx.has(key)) {
				burnTransferMapByTx.set(key, []);
			}
			burnTransferMapByTx.get(key)!.push({
				from: transfer.from,
				value: transfer.value,
				logIndex: transfer.logIndex
			});
		}
	}
	console.log(`Indexed ${allTransferEvents.length} Transfer events into ${mintTransferMapByTx.size} mint tx hashes and ${burnTransferMapByTx.size} burn tx hashes`);

	// Log memory usage warning if transfer maps are very large
	if (mintTransferMapByTx.size > 100000 || burnTransferMapByTx.size > 100000) {
		console.warn(`[main] Warning: Transfer maps have ${mintTransferMapByTx.size} mint entries and ${burnTransferMapByTx.size} burn entries. This may use significant memory.`);
	}

	// Process Mint events
	console.log("Processing Mint events...");
	for (const decoded of allMintEvents) {
		totalMintEvents++;
		const txHash = decoded.transactionHash.toLowerCase();
		const availableTransfers = mintTransferMapByTx.get(txHash) || [];

		if (availableTransfers.length > 0) {
			// Precise matching: find the transfer with the closest log index to the mint's log index
			// Prefer transfers that come after the mint event (logIndex >= mint.logIndex)
			// If none found, use the closest one overall
			let bestMatch: {to: string, value: bigint, logIndex: number} | null = null;
			let bestDistance = Infinity;

			for (const transfer of availableTransfers) {
				const distance = Math.abs(transfer.logIndex - decoded.logIndex);
				// Prefer transfers that come after the mint (they're more likely to be the result)
				if (transfer.logIndex >= decoded.logIndex) {
					if (distance < bestDistance) {
						bestMatch = transfer;
						bestDistance = distance;
					}
				}
			}

			// If no transfer found after mint, use the closest one overall
			if (!bestMatch) {
				for (const transfer of availableTransfers) {
					const distance = Math.abs(transfer.logIndex - decoded.logIndex);
					if (distance < bestDistance) {
						bestMatch = transfer;
						bestDistance = distance;
					}
				}
			}

			if (bestMatch) {
				// Write matched mint event as CSV row with Transfer event value as amount
				const row = [
					'mint',
					decoded.transactionHash,
					decoded.blockNumber.toString(),
					bestMatch.value.toString(),
					decoded.amount0.toString(),
					decoded.amount1.toString(),
					bestMatch.to,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
				matchedMintCount++;

				// Log if there are multiple mint transfers and we had to choose
				if (availableTransfers.length > 1) {
					console.log(`[main] Matched mint (logIndex=${decoded.logIndex}) with transfer (logIndex=${bestMatch.logIndex}, distance=${bestDistance}, value=${bestMatch.value.toString()}) in tx ${decoded.transactionHash}`);
				}

				// Remove the matched transfer to avoid reusing it (optional, but helps with multiple mints)
				const transferIndex = availableTransfers.indexOf(bestMatch);
				if (transferIndex !== -1) {
					availableTransfers.splice(transferIndex, 1);
				}
			} else {
				unmatchedMintCount++;
				// Write unmatched mint event with amount=0
				const row = [
					'mint',
					decoded.transactionHash,
					decoded.blockNumber.toString(),
					'0',
					decoded.amount0.toString(),
					decoded.amount1.toString(),
					decoded.sender, // Use sender as user when no transfer match
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					outputFile.once('drain', () => {});
				}
			}
		} else {
			unmatchedMintCount++;
			// Write unmatched mint event with amount=0
			const row = [
				'mint',
				decoded.transactionHash,
				decoded.blockNumber.toString(),
				'0',
				decoded.amount0.toString(),
				decoded.amount1.toString(),
				decoded.sender, // Use sender as user when no transfer match
			];
			const csvRow = row.map(escapeCsvValue).join(',') + '\n';
			if (!outputFile.write(csvRow)) {
				outputFile.once('drain', () => {});
			}
		}
	}
	console.log(`Processed ${totalMintEvents} Mint events (${matchedMintCount} matched, ${unmatchedMintCount} unmatched)`);

	// Process Burn events
	console.log("Processing Burn events...");
	for (const decoded of allBurnEvents) {
		totalBurnEvents++;
		const txHash = decoded.transactionHash.toLowerCase();
		const availableTransfers = burnTransferMapByTx.get(txHash) || [];

		if (availableTransfers.length > 0) {
			// Precise matching: find the transfer with the closest log index to the burn's log index
			// Prefer transfers that come after the burn event (logIndex >= burn.logIndex)
			// If none found, use the closest one overall
			let bestMatch: {from: string, value: bigint, logIndex: number} | null = null;
			let bestDistance = Infinity;

			for (const transfer of availableTransfers) {
				const distance = Math.abs(transfer.logIndex - decoded.logIndex);
				// Prefer transfers that come after the burn (they're more likely to be the result)
				if (transfer.logIndex >= decoded.logIndex) {
					if (distance < bestDistance) {
						bestMatch = transfer;
						bestDistance = distance;
					}
				}
			}

			// If no transfer found after burn, use the closest one overall
			if (!bestMatch) {
				for (const transfer of availableTransfers) {
					const distance = Math.abs(transfer.logIndex - decoded.logIndex);
					if (distance < bestDistance) {
						bestMatch = transfer;
						bestDistance = distance;
					}
				}
			}

			if (bestMatch) {
				// Write matched burn event as CSV row with Transfer event value as amount
				const row = [
					'burn',
					decoded.transactionHash,
					decoded.blockNumber.toString(),
					bestMatch.value.toString(),
					decoded.amount0.toString(),
					decoded.amount1.toString(),
					decoded.to,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
				matchedBurnCount++;

				// Log if there are multiple burn transfers and we had to choose
				if (availableTransfers.length > 1) {
					console.log(`[main] Matched burn (logIndex=${decoded.logIndex}) with transfer (logIndex=${bestMatch.logIndex}, distance=${bestDistance}, value=${bestMatch.value.toString()}) in tx ${decoded.transactionHash}`);
				}

				// Remove the matched transfer to avoid reusing it (optional, but helps with multiple burns)
				const transferIndex = availableTransfers.indexOf(bestMatch);
				if (transferIndex !== -1) {
					availableTransfers.splice(transferIndex, 1);
				}
			} else {
				unmatchedBurnCount++;
				// Write unmatched burn event with amount=0
				const row = [
					'burn',
					decoded.transactionHash,
					decoded.blockNumber.toString(),
					'0',
					decoded.amount0.toString(),
					decoded.amount1.toString(),
					decoded.to,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					outputFile.once('drain', () => {});
				}
			}
		} else {
			unmatchedBurnCount++;
			// Write unmatched burn event with amount=0
			const row = [
				'burn',
				decoded.transactionHash,
				decoded.blockNumber.toString(),
				'0',
				decoded.amount0.toString(),
				decoded.amount1.toString(),
				decoded.to,
			];
			const csvRow = row.map(escapeCsvValue).join(',') + '\n';
			if (!outputFile.write(csvRow)) {
				outputFile.once('drain', () => {});
			}
		}
	}
	console.log(`Processed ${totalBurnEvents} Burn events (${matchedBurnCount} matched, ${unmatchedBurnCount} unmatched)`);

	// Close the file
	outputFile.end();

	// Wait for file to finish writing
	await new Promise((resolve) => outputFile.on('finish', resolve));

	console.log(`[main] Results written to ${outputPath}`);
	console.log(`Total events: ${matchedMintCount + matchedBurnCount} (${matchedMintCount} matched mints, ${matchedBurnCount} matched burns)`);
	console.log(`Unmatched events: ${unmatchedMintCount} mints, ${unmatchedBurnCount} burns`);
	console.log("[main] Main function completed successfully");
}

console.log("[startup] Script starting...");
main().catch((error) => {
	console.error("[startup] Fatal error in main:", error);
	process.exit(1);
});
