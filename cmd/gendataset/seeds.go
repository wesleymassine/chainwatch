package main

// Seed addresses: real wallets that were active when this list was built.
//
// They exist so a demo against a live chain emits events within seconds. 500k
// random addresses would be realistic but would never match anything, and a
// monitor that prints nothing looks broken even when it is correct.
//
// Every entry was verified by scanning recent blocks and counting how many
// transactions touched it, then confirming with eth_getCode that it is an
// externally owned account. Contracts were excluded on purpose: the dataset is
// meant to represent user wallets, and seeding it with USDT or a Uniswap router
// would mostly produce zero-value native transfers.
//
// Verified 2026-08-28. Coverage is "blocks touched / blocks scanned"; these
// figures decay over time, which is exactly why the file records when it was
// measured.
var seeds = []string{
	// Ethereum mainnet — 40 blocks scanned around block 25,854,638.
	"0x28c6c06298d514db089934071355e5743bf21d60", // 36/40 blocks
	"0x05ff6964d21e5dae3b1010d5ae0465b3c450f381", // 34/40
	"0xdfd5293d8e347dfe59e90efd55b2956a1343963d", // 33/40
	"0x21a31ee1afc51d94c2efccaa2092ad1028285549", // 32/40
	"0x56eddb7aa87536c09ccc2793473599fd21a8b17f", // 31/40
	"0xf30ba13e4b04ce5dc4d254ae5fa95477800f0eb0", // 31/40
	"0x6872b6630a3afcd3117191a8403c2002e13df7de", // 30/40
	"0x9696f59e4d72e237be84ffd425dcad154bf96976", // 29/40
	"0xd47a1bdc6872ad2fd16e50149baa9924c653e624", // 27/40
	"0xdaa526086787d9debe1d7f3ffdb1fe50cf8687f4", // 26/40
	"0x264bd8291fae1d75db2c5f573b07faa6715997b5", // 22/40
	"0x18e296053cbdf986196903e889b7dca7a73882f6", // 22/40
	"0x559432e18b281731c054cd703d4b49872be4ed53", // 21/40
	"0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23", // 21/40
	"0x300773969b499b9768267e0c5a049f7751de79e7", // 17/40
	"0xe8832a868c091263ed190a9f4be304a03895dd91", // 14/40
	"0x46340b20830761efd32832a74d7169b29feb9758", // 14/40
	"0x477b8d5ef7c2c42db84deb555419cd817c336b6f", //  2/40

	// Arbitrum One — 300 blocks scanned around block 499,294,701.
	"0x2976000163f8eefcd2962b54891d7c8dd31cb3b8", // 177/300 blocks
	"0xe1acc9d6d65b24be793aff96fc6caa95ffb34ab3", //  51/300
}
