# ST0X Rewards Script

A TypeScript-based blockchain data extraction tool for tracking liquidity pool participation on the Base network. This tool queries blockchain events to generate CSV ledgers of liquidity provider activity for reward distribution calculations.

## Overview

This repository contains two versions for processing different types of liquidity pools:

- **v2.ts** - Processes Uniswap V2-style pools with simple Mint/Burn events
- **v3.ts** - Processes Uniswap V3-style concentrated liquidity pools with complex position management

Both scripts query blockchain logs via the HyperSync API, match related events, and output structured CSV files for reward calculations.

## Features

- ✅ Memory-efficient streaming log processing
- ✅ Robust retry logic with exponential backoff
- ✅ Event correlation across transaction boundaries
- ✅ CSV output for easy data analysis
- ✅ Support for both V2 and V3 liquidity pool types

## Prerequisites

- Node.js (v18 or higher recommended)
- npm or yarn
- HyperSync API key (obtain from [HyperSync](https://hypersync.xyz/))

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd st0x-rewards-script
```

2. Install dependencies:
```bash
npm install
```

3. Create environment file:
```bash
cp .env.example .env
```

4. Add your HyperSync API key to `.env`:
```
HYPERSYNC_API_KEY=your_api_key_here
```

## Usage

### Running V2 Script

The default script processes V2-style pools:

```bash
npm start
```

This executes `src/v2.ts` which:
- Queries Transfer, Mint, and Burn events
- Matches events by transaction hash
- Outputs to CSV with columns: type, transactionHash, blockNumber, amount, amount0, amount1, user

### Running V3 Script

To process V3-style concentrated liquidity pools:

```bash
npx ts-node src/v3.ts
```

This executes `src/v3.ts` which:
- Queries Transfer, Mint, Collect, IncreaseLiquidity, and DecreaseLiquidity events
- Performs sophisticated event matching by log index
- Outputs comprehensive liquidity tracking data

## Configuration

### V2 Configuration

Edit `src/v2.ts` to modify:

```typescript
const FROM_BLOCK = 39557809;
const TO_BLOCK = 40719263;
const CL_POOL = "0xb804FA2e3631465455594538067001E7A0f83D37";
const NFT_CONTRACT = "0xb804FA2e3631465455594538067001E7A0f83D37";
```

### V3 Configuration

Edit `src/v3.ts` to modify:

```typescript
const FROM_BLOCK = 38913515;
const TO_BLOCK = 40249153;
const CL_POOL = "0x40a8e39AbA67debEdab94F76D21114AB39909c5a";
const NFT_CONTRACT = "0xa990c6a764b73bf43cee5bb40339c3322fb9d55f";
```

## Important Notes

### Block Range Limitations

The scripts only capture events within the specified FROM_BLOCK to TO_BLOCK range. This means:

⚠️ **If a position was created BEFORE FROM_BLOCK, you won't see the initial liquidity addition**

This can cause:
- Decrease events without corresponding increase events
- Imbalanced liquidity calculations (appears to decrease more than increased)
- Incomplete position history

**Solutions:**
1. Set FROM_BLOCK to the pool creation block to capture complete history
2. Use a blockchain explorer to find the pool creation block
3. Adjust FROM_BLOCK based on when you need to start tracking

### Event Deduplication (V3)

V3 pools emit events from both the Pool contract and Position Manager contract:
- **Mint (pool) + IncreaseLiquidity (manager)** = Same operation
- **Burn (pool) + DecreaseLiquidity (manager)** = Same operation

The script outputs **only IncreaseLiquidity and DecreaseLiquidity** to avoid double-counting. Pool events (Mint/Burn) are used internally for matching and user attribution but not written as separate CSV rows.

## Output Format

### V2 Output

CSV with the following columns:
- `type` - Event type (mint/burn)
- `transactionHash` - Transaction hash
- `blockNumber` - Block number
- `amount` - NFT token amount
- `amount0` - Pool token0 amount
- `amount1` - Pool token1 amount
- `user` - Address of the user

### V3 Output

CSV with the following event types:

**increaseLiquidity** - Liquidity added to position
- `type` = 'increaseLiquidity'
- `transactionHash` - Transaction hash
- `blockNumber` - Block number
- `amount` - Liquidity amount added
- `amount0` - Token0 amount
- `amount1` - Token1 amount
- `user` - User address (from Transfer event)

**decreaseLiquidity** - Liquidity removed from position
- `type` = 'decreaseLiquidity'
- `transactionHash` - Transaction hash
- `blockNumber` - Block number
- `amount` - Liquidity amount removed
- `amount0` - Token0 amount
- `amount1` - Token1 amount
- `user` - User address (from Collect event)

**collect** - Fee collection (without liquidity removal)
- `type` = 'collect'
- `transactionHash` - Transaction hash
- `blockNumber` - Block number
- `amount` - 0 (no liquidity change)
- `amount0` - Token0 fees collected
- `amount1` - Token1 fees collected
- `user` - Recipient address

## Architecture

### HyperSync Integration

Both scripts use the HyperSync API for efficient blockchain log querying:
- Chain ID: 8453 (Base network)
- Streaming callback architecture for memory efficiency
- Automatic pagination via `next_block` parameter

### Event Matching Logic

**V2 Approach:**
- Matches Transfer events with corresponding Mint/Burn events
- Groups by transaction hash
- Validates log index ordering

**V3 Approach:**
- More complex matching for concentrated liquidity positions
- Correlates IncreaseLiquidity/DecreaseLiquidity with Mint/Collect
- Tracks position-level changes across multiple transactions

### Retry Logic

Both scripts implement exponential backoff:
- Max 5 retry attempts
- Delays: 1s, 2s, 4s, 8s, 10s
- Automatic recovery from transient API failures

## Development

### With Nix

This repository includes a Nix flake for reproducible development:

```bash
nix develop
```

### Manual Setup

```bash
npm install
npm run build  # If you add a build script
```

## Technology Stack

- **TypeScript** - Type-safe JavaScript
- **ethers.js v6** - Ethereum interaction and ABI decoding
- **axios** - HTTP client for API requests
- **HyperSync API** - Efficient blockchain log queries
- **ts-node** - TypeScript execution runtime

## Dependencies

### Core Dependencies
- `ethers` - Blockchain interaction
- `axios` - HTTP requests
- `dotenv` - Environment variable management

### Additional Features
- `@safe-global/protocol-kit` - Safe wallet integration
- `@wagmi/core` - Ethereum wallet connections
- `viem` - Alternative Ethereum library
- `pinata-web3` - IPFS integration

## Troubleshooting

### API Rate Limiting

If you encounter rate limiting:
- Reduce the block range size
- Add delays between requests
- Contact HyperSync for increased limits

### Memory Issues

For large block ranges:
- Process data in smaller chunks
- Adjust the `next_block` pagination
- Monitor memory usage with `node --max-old-space-size=4096`

### Event Matching Failures

If events aren't matching correctly:
- Verify contract addresses
- Check ABI definitions match deployed contracts
- Ensure block range includes complete transactions

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

[Add license information]

## Support

For issues or questions:
- Open a GitHub issue
- Contact the development team

## Roadmap

Potential improvements:
- [ ] CLI interface for dynamic configuration
- [ ] Unit tests for event matching logic
- [ ] Configuration file support
- [ ] Real-time streaming mode
- [ ] Support for additional networks
- [ ] Automated reward calculation
- [ ] Database output option
