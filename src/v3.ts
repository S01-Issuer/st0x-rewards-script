import { ethers } from 'ethers';
import axios from 'axios';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();

const FROM_BLOCK = 38913515;
const TO_BLOCK = 40249153;

const HYPERSYNC_API_KEY = process.env.HYPERSYNC_API_KEY;
// Using chain-id 1 for Ethereum mainnet (adjust if needed)
const CHAIN_ID = 8453;
const HYPERSYNC_URL = `https://${CHAIN_ID}.hypersync.xyz/query`;

const CL_POOL = "0x40a8e39AbA67debEdab94F76D21114AB39909c5a";
// Based on the images, Transfer events are from the NFT contract
const NFT_CONTRACT = "0xa990c6a764b73bf43cee5bb40339c3322fb9d55f";

// Mint event signature: Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
const MINT_EVENT_SIGNATURE = ethers.id("Mint(address,address,int24,int24,uint128,uint256,uint256)");
// Collect event signature: Collect(address indexed owner, address recipient, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount0, uint128 amount1)
const COLLECT_EVENT_SIGNATURE = ethers.id("Collect(address,address,int24,int24,uint128,uint128)");
// Transfer event signature: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
const TRANSFER_EVENT_SIGNATURE = ethers.id("Transfer(address,address,uint256)");
// IncreaseLiquidity event signature: IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
const INCREASE_LIQUIDITY_EVENT_SIGNATURE = ethers.id("IncreaseLiquidity(uint256,uint128,uint256,uint256)");
// DecreaseLiquidity event signature: DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
const DECREASE_LIQUIDITY_EVENT_SIGNATURE = ethers.id("DecreaseLiquidity(uint256,uint128,uint256,uint256)");

console.log('TRANSFER_EVENT_SIGNATURE : ', TRANSFER_EVENT_SIGNATURE);

interface MintEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	sender: string;
	owner: string;
	tickLower: bigint;
	tickUpper: bigint;
	amount: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface CollectEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	owner: string;
	recipient: string;
	tickLower: bigint;
	tickUpper: bigint;
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
	tokenId: bigint;
	timestamp: number | null;
}

interface MatchedMintEvent {
	mint: MintEvent;
	transfer: TransferEvent;
}

interface MatchedCollectEvent {
	collect: CollectEvent;
	transfer: TransferEvent;
}

interface IncreaseLiquidityEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	tokenId: bigint;
	liquidity: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface DecreaseLiquidityEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	tokenId: bigint;
	liquidity: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
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
			console.error(`[fetchAndProcessLogs] Failed to fetch after ${maxRetries} attempts. Breaking loop.`);
			break;
		}
	}

	console.log(`[fetchAndProcessLogs] Completed. Total processed logs: ${totalProcessed}`);
	return totalProcessed;
}

function decodeMintEvent(log: any): MintEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature)
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
			owner: decoded.args.owner.toLowerCase(),
			tickLower: decoded.args.tickLower,
			tickUpper: decoded.args.tickUpper,
			amount: decoded.args.amount,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeMintEvent] Successfully decoded Mint event: tx=${decodedEvent.transactionHash}, sender=${decodedEvent.sender}, owner=${decodedEvent.owner}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeMintEvent] Error decoding Mint event:", error);
		return null;
	}
}

function decodeCollectEvent(log: any): CollectEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Collect(address indexed owner, address recipient, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount0, uint128 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1, topic2, topic3 are indexed)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeCollectEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			owner: decoded.args.owner.toLowerCase(),
			recipient: decoded.args.recipient.toLowerCase(),
			tickLower: decoded.args.tickLower,
			tickUpper: decoded.args.tickUpper,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeCollectEvent] Successfully decoded Collect event: tx=${decodedEvent.transactionHash}, owner=${decodedEvent.owner}, recipient=${decodedEvent.recipient}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeCollectEvent] Error decoding Collect event:", error);
		return null;
	}
}

function decodeTransferEvent(log: any): TransferEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
		]);
		
		// Construct topics array (all topics are indexed for Transfer)
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
			tokenId: decoded.args.tokenId,
			timestamp: log.timestamp,
		};
		console.log(`[decodeTransferEvent] Successfully decoded Transfer event: tx=${decodedEvent.transactionHash}, from=${decodedEvent.from}, to=${decodedEvent.to}, tokenId=${decodedEvent.tokenId.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeTransferEvent] Error decoding Transfer event:", error);
		return null;
	}
}

function decodeIncreaseLiquidityEvent(log: any): IncreaseLiquidityEvent | null {
	try {
		const iface = new ethers.Interface([
			"event IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1 is indexed tokenId)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeIncreaseLiquidityEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			tokenId: decoded.args.tokenId,
			liquidity: decoded.args.liquidity,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeIncreaseLiquidityEvent] Successfully decoded IncreaseLiquidity event: tx=${decodedEvent.transactionHash}, tokenId=${decodedEvent.tokenId.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeIncreaseLiquidityEvent] Error decoding IncreaseLiquidity event:", error);
		return null;
	}
}

function decodeDecreaseLiquidityEvent(log: any): DecreaseLiquidityEvent | null {
	try {
		const iface = new ethers.Interface([
			"event DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1 is indexed tokenId)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeDecreaseLiquidityEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			tokenId: decoded.args.tokenId,
			liquidity: decoded.args.liquidity,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeDecreaseLiquidityEvent] Successfully decoded DecreaseLiquidity event: tx=${decodedEvent.transactionHash}, tokenId=${decodedEvent.tokenId.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeDecreaseLiquidityEvent] Error decoding DecreaseLiquidity event:", error);
		return null;
	}
}

// Simple CSV escaping - wrap in quotes if contains comma, quote, or newline
function escapeCsvValue(value: string): string {
	if (value.includes(',') || value.includes('"') || value.includes('\n')) {
		return '"' + value.replace(/"/g, '""') + '"';
	}
	return value;
}

async function main() {
	console.log("[main] Starting main function");
	console.log(`[main] Configuration: FROM_BLOCK=${FROM_BLOCK}, TO_BLOCK=${TO_BLOCK}`);
	console.log(`[main] CL_POOL=${CL_POOL}, NFT_CONTRACT=${NFT_CONTRACT}`);
	
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
	let totalCollectEvents = 0;
	let matchedMintCount = 0;
	let unmatchedMintCount = 0;
	let totalIncreaseLiquidityEvents = 0;
	let totalDecreaseLiquidityEvents = 0;
	let matchedIncreaseLiquidityCount = 0;
	let matchedDecreaseLiquidityCount = 0;

	// Step 1: Fetch and index Transfer events (minimal memory - just tx hash -> first transfer)
	console.log("Querying Transfer events from NFT contract...");
	const transferMapByTx = new Map<string, TransferEvent>();
	let transferCount = 0;
	
	await fetchAndProcessLogs(
		HYPERSYNC_URL,
		NFT_CONTRACT,
		TRANSFER_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK,
		(log) => {
			const decoded = decodeTransferEvent(log);
			if (decoded) {
				const key = decoded.transactionHash.toLowerCase();
				// Only store first transfer per tx for matching
				if (!transferMapByTx.has(key)) {
					transferMapByTx.set(key, decoded);
				}
				transferCount++;
			}
		}
	);
	console.log(`Processed ${transferCount} Transfer events, indexed ${transferMapByTx.size} unique transaction hashes`);
	
	// Log memory usage warning if transfer map is very large
	if (transferMapByTx.size > 100000) {
		console.warn(`[main] Warning: Transfer map has ${transferMapByTx.size} entries. This may use significant memory.`);
	}

	// Step 2: Fetch and index Mint events (store for matching with IncreaseLiquidity)
	console.log("Querying Mint events from pool contract...");
	// Store arrays of mint events per transaction, sorted by log index
	const mintMapByTx = new Map<string, Array<{ mint: MintEvent; user: string; logIndex: number }>>();
	await fetchAndProcessLogs(
		HYPERSYNC_URL,
		CL_POOL,
		MINT_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK,
		(log) => {
			const decoded = decodeMintEvent(log);
			if (decoded) {
				totalMintEvents++;
				const txHash = decoded.transactionHash.toLowerCase();
				const matchingTransfer = transferMapByTx.get(txHash);
				
				if (matchingTransfer) {
					// Store mint event with user for matching with IncreaseLiquidity
					if (!mintMapByTx.has(txHash)) {
						mintMapByTx.set(txHash, []);
					}
					mintMapByTx.get(txHash)!.push({ mint: decoded, user: matchingTransfer.to, logIndex: decoded.logIndex });
					
					// Write matched mint event as CSV row
					const row = [
						'mint',
						decoded.transactionHash,
						decoded.blockNumber.toString(),
						decoded.amount.toString(),
						decoded.amount0.toString(),
						decoded.amount1.toString(),
						matchingTransfer.to,
					];
					const csvRow = row.map(escapeCsvValue).join(',') + '\n';
					if (!outputFile.write(csvRow)) {
						// Handle backpressure - wait for drain event
						outputFile.once('drain', () => {});
					}
					matchedMintCount++;
				} else {
					unmatchedMintCount++;
				}
			}
		}
	);
	// Sort mint events by log index within each transaction
	for (const mints of mintMapByTx.values()) {
		mints.sort((a, b) => a.logIndex - b.logIndex);
	}
	console.log(`Processed ${totalMintEvents} Mint events (${matchedMintCount} matched, ${unmatchedMintCount} unmatched)`);

	// Step 3: Fetch and index Collect events (store for matching with DecreaseLiquidity)
	console.log("Querying Collect events from pool contract...");
	// Store arrays of collect events per transaction, sorted by log index
	const collectMapByTx = new Map<string, Array<CollectEvent>>();
	await fetchAndProcessLogs(
		HYPERSYNC_URL,
		CL_POOL,
		COLLECT_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK,
		(log) => {
			const decoded = decodeCollectEvent(log);
			if (decoded) {
				totalCollectEvents++;
				// Store collect event for matching with DecreaseLiquidity
				const txHash = decoded.transactionHash.toLowerCase();
				if (!collectMapByTx.has(txHash)) {
					collectMapByTx.set(txHash, []);
				}
				collectMapByTx.get(txHash)!.push(decoded);
				
				// Write collect event as CSV row
				const row = [
					'collect',
					decoded.transactionHash,
					decoded.blockNumber.toString(),
					'0',
					decoded.amount0.toString(),
					decoded.amount1.toString(),
					decoded.recipient,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
			}
		}
	);
	// Sort collect events by log index within each transaction
	for (const collects of collectMapByTx.values()) {
		collects.sort((a, b) => a.logIndex - b.logIndex);
	}
	console.log(`Processed ${totalCollectEvents} Collect events`);

	// Step 4: Fetch IncreaseLiquidity events and match with Mint events by log index order
	console.log("Querying IncreaseLiquidity events from pool contract...");
	// Track matched IncreaseLiquidity events per transaction to match by position
	const increaseLiquidityMapByTx = new Map<string, Array<IncreaseLiquidityEvent>>();
	await fetchAndProcessLogs(
		HYPERSYNC_URL,
		CL_POOL,
		INCREASE_LIQUIDITY_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK,
		(log) => {
			const decoded = decodeIncreaseLiquidityEvent(log);
			if (decoded) {
				totalIncreaseLiquidityEvents++;
				const txHash = decoded.transactionHash.toLowerCase();
				if (!increaseLiquidityMapByTx.has(txHash)) {
					increaseLiquidityMapByTx.set(txHash, []);
				}
				increaseLiquidityMapByTx.get(txHash)!.push(decoded);
			}
		}
	);
	// Sort IncreaseLiquidity events by log index within each transaction and match with Mint events
	for (const [txHash, increaseLiquidityEvents] of increaseLiquidityMapByTx.entries()) {
		increaseLiquidityEvents.sort((a, b) => a.logIndex - b.logIndex);
		const mintEvents = mintMapByTx.get(txHash) || [];
		
		// Match by position (assuming same order as log index)
		for (let i = 0; i < increaseLiquidityEvents.length; i++) {
			const increaseLiquidity = increaseLiquidityEvents[i];
			const matchingMint = mintEvents[i];
			
			if (matchingMint) {
				// Write IncreaseLiquidity event as CSV row with user from Mint event
				const row = [
					'increaseLiquidity',
					increaseLiquidity.transactionHash,
					increaseLiquidity.blockNumber.toString(),
					increaseLiquidity.liquidity.toString(),
					increaseLiquidity.amount0.toString(),
					increaseLiquidity.amount1.toString(),
					matchingMint.user,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
				matchedIncreaseLiquidityCount++;
			} else {
				console.warn(`[main] IncreaseLiquidity event at tx ${increaseLiquidity.transactionHash} (logIndex ${increaseLiquidity.logIndex}) has no matching Mint event at position ${i}`);
			}
		}
	}
	console.log(`Processed ${totalIncreaseLiquidityEvents} IncreaseLiquidity events (${matchedIncreaseLiquidityCount} matched)`);

	// Step 5: Fetch DecreaseLiquidity events and match with Collect events by log index order
	console.log("Querying DecreaseLiquidity events from pool contract...");
	// Track matched DecreaseLiquidity events per transaction to match by position
	const decreaseLiquidityMapByTx = new Map<string, Array<DecreaseLiquidityEvent>>();
	await fetchAndProcessLogs(
		HYPERSYNC_URL,
		CL_POOL,
		DECREASE_LIQUIDITY_EVENT_SIGNATURE,
		FROM_BLOCK,
		TO_BLOCK,
		(log) => {
			const decoded = decodeDecreaseLiquidityEvent(log);
			if (decoded) {
				totalDecreaseLiquidityEvents++;
				const txHash = decoded.transactionHash.toLowerCase();
				if (!decreaseLiquidityMapByTx.has(txHash)) {
					decreaseLiquidityMapByTx.set(txHash, []);
				}
				decreaseLiquidityMapByTx.get(txHash)!.push(decoded);
			}
		}
	);
	// Sort DecreaseLiquidity events by log index within each transaction and match with Collect events
	for (const [txHash, decreaseLiquidityEvents] of decreaseLiquidityMapByTx.entries()) {
		decreaseLiquidityEvents.sort((a, b) => a.logIndex - b.logIndex);
		const collectEvents = collectMapByTx.get(txHash) || [];
		
		// Match by position (assuming same order as log index)
		for (let i = 0; i < decreaseLiquidityEvents.length; i++) {
			const decreaseLiquidity = decreaseLiquidityEvents[i];
			const matchingCollect = collectEvents[i];
			
			if (matchingCollect) {
				// Write DecreaseLiquidity event as CSV row with recipient from Collect event
				const row = [
					'decreaseLiquidity',
					decreaseLiquidity.transactionHash,
					decreaseLiquidity.blockNumber.toString(),
					decreaseLiquidity.liquidity.toString(),
					decreaseLiquidity.amount0.toString(),
					decreaseLiquidity.amount1.toString(),
					matchingCollect.recipient,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
				matchedDecreaseLiquidityCount++;
			} else {
				console.warn(`[main] DecreaseLiquidity event at tx ${decreaseLiquidity.transactionHash} (logIndex ${decreaseLiquidity.logIndex}) has no matching Collect event at position ${i}`);
			}
		}
	}
	console.log(`Processed ${totalDecreaseLiquidityEvents} DecreaseLiquidity events (${matchedDecreaseLiquidityCount} matched)`);

	// Close the file
	outputFile.end();

	// Wait for file to finish writing
	await new Promise((resolve) => outputFile.on('finish', resolve));

	console.log(`[main] Results written to ${outputPath}`);
	console.log(`Total events: ${matchedMintCount + totalCollectEvents + matchedIncreaseLiquidityCount + matchedDecreaseLiquidityCount} (${matchedMintCount} matched mints, ${totalCollectEvents} collects, ${matchedIncreaseLiquidityCount} matched increaseLiquidity, ${matchedDecreaseLiquidityCount} matched decreaseLiquidity)`);
	console.log("[main] Main function completed successfully");
}

console.log("[startup] Script starting...");
main().catch((error) => {
	console.error("[startup] Fatal error in main:", error);
	process.exit(1);
});
