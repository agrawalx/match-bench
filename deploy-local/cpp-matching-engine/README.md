# C++ Limit Order Book (LOB) Matching Engine

A high-performance C++20 Limit Order Book (LOB) designed with modern systems programming and exchange microstructure standards in mind. The project is fully structured with a CMake build system and contains an extensive test suite of 56 test cases built using Google Test, verifying correctness and performance under randomized stress scenarios.

---

## 🚀 Key Features

* **Price-Time Priority (FIFO)**: Matches bids and asks in accordance with standard exchange priority rules.
* **Order Types Supported**:
  * **Good-Till-Cancel (GTC)**: Standard resting limit orders.
  * **Fill-and-Kill (FAK)**: Immediate matching execution with automatic remainder cancellation (no resting portion).
  * **Market Orders**: Executes immediately against liquidity, sweeping multiple price levels if necessary.
* **Optimized Data Structures**:
  * $O(1)$ amortized order cancellation/modification by mapping Order IDs directly to queue iterators via a hash map (`std::unordered_map`).
  * $O(\log N)$ price level insertion using balanced binary search trees (`std::map`).
* **Automated Correctness Verification**:
  * 56 custom Google Test cases validating engine logic.
  * Property-based invariant testing checking internal state validity after every single execution step.
  * 1,000,000 operation randomized stress test verifying stability and safety under extreme trade flows.

---

## 📁 Project Structure

```
├── CMakeLists.txt              # Cross-platform build configuration
├── .gitignore                  # Professional git exclusions (build/ and binary outputs)
├── include/                    # Header files (Declarations)
│   ├── types.h                 # Quant price/qty typings and LevelInfo structures
│   ├── order.h                 # Order and OrderModify definitions
│   ├── trade.h                 # Trade execution structures
│   └── orderbook.h             # Orderbook matching engine declaration
├── src/                        # Implementation files
│   ├── orderbook.cpp           # Main matching logic
│   └── main.cpp                # Executable entry point
├── tests/                      # Testing directory
│   ├── test_order.cpp          # Unit tests for order state changes
│   ├── test_orderbook_basic.cpp# Basic FIFO, price priority, cancel/replace matching
│   ├── test_orderbook_types.cpp# Type-specific rules (Market and FAK executions)
│   └── test_invariants.cpp     # Property-based testing & 1M-operation stress test
└── README.md                   
```

---

## 🛠️ Build & Run Instructions

This project uses **CMake** to automatically pull dependencies (Google Test) and compile the targets.

### Prerequisites
Make sure you have CMake (>= 3.14) and a C++ compiler supporting C++20 (GCC/MinGW, Clang, or MSVC) installed.

### 1. Configure the Build
Generate compiler-specific build files (this will automatically fetch and compile Google Test on the first run):
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
```

### 2. Compile the Targets
```bash
cmake --build build -j4
```

### 3. Run the Test Suite
Verify everything is working correctly with CTest:
```bash
ctest --test-dir build --output-on-failure
```

### 4. Run the Main Executable
```bash
./build/orderbook_main.exe
```

---

## 🧪 Testing Strategy 

This project includes institutional-grade testing systems:

### 1. Property-Based Invariant Assertions
In `tests/test_invariants.cpp`, the book verifies structural invariants after every single trade, add, cancel, or modify action:
* **Sorted Bid Stack**: Best bid (highest price) is always at the front; bids are strictly sorted descending.
* **Sorted Ask Stack**: Best ask (lowest price) is always at the front; asks are strictly sorted ascending.
* **No Crossed Spread**: Best bid price is strictly less than the best ask price (otherwise a match should have occurred).
* **No Zero-Volume Levels**: Empty price levels are cleaned up immediately to prevent memory leaks and phantom matching.

### 2. Randomized Fuzz & Stress Testing
* **10,000 Action Loop**: Executes a deterministic seed sequence of random order insertions, cancels, and modifies, checking every single invariant rule after each step.
* **1,000,000 Action Stress Test**: Fires 1 million randomized orders (GTC, FAK, Market), cancels, and replaces to verify that the matching engine has zero segmentation faults, memory corruption, or performance bottlenecks under heavy volumes (completes in ~21 seconds).
