#include <gtest/gtest.h>
#include "orderbook.h"

static OrderPointer MakeGTC(OrderId id, Side side, Price price, Quantity qty) {
    return std::make_shared<Order>(OrderType::GoodTillCancel, id, side, price, qty);
}

static OrderPointer MakeFAK(OrderId id, Side side, Price price, Quantity qty) {
    return std::make_shared<Order>(OrderType::FillAndKill, id, side, price, qty);
}

static OrderPointer MakeMarket(OrderId id, Side side, Quantity qty) {
    return std::make_shared<Order>(id, side, qty);
}


TEST(FillAndKillTest, FAK_NoMatch_RejectedImmediately) {
    Orderbook book;
    
    auto trades = book.AddOrder(MakeFAK(1, Side::Buy, 100, 50));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 0); 
}

TEST(FillAndKillTest, FAK_NoMatchBecauseSpread_Rejected) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 101, 50));
    
    auto trades = book.AddOrder(MakeFAK(2, Side::Buy, 99, 50));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 1); 
}

TEST(FillAndKillTest, FAK_FullMatch_ExecutesNormally) {

    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    
    auto trades = book.AddOrder(MakeFAK(2, Side::Buy, 100, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 50);
    EXPECT_EQ(book.Size(), 0);
}

TEST(FillAndKillTest, FAK_PartialMatch_RemainderCancelled) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 30));
    
    auto trades = book.AddOrder(MakeFAK(2, Side::Buy, 100, 100));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 30); 
    EXPECT_EQ(book.Size(), 0);
    auto infos = book.GetOrderInfos();
    EXPECT_TRUE(infos.GetBids().empty());   
    EXPECT_TRUE(infos.GetAsks().empty());   
}

TEST(FillAndKillTest, FAK_Sell_PartialMatch_RemainderCancelled) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 30));
    
    auto trades = book.AddOrder(MakeFAK(2, Side::Sell, 100, 100));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetAskTrade().quantity_, 30);
    EXPECT_EQ(book.Size(), 0); 
}

TEST(MarketOrderTest, MarketBuy_EmptyBook_Rejected) {
    Orderbook book;
    
    auto trades = book.AddOrder(MakeMarket(1, Side::Buy, 100));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 0);
}

TEST(MarketOrderTest, MarketSell_EmptyBook_Rejected) {
    Orderbook book;
    
    auto trades = book.AddOrder(MakeMarket(1, Side::Sell, 100));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 0);
}

TEST(MarketOrderTest, MarketBuy_MatchesBestAsk) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    
    auto trades = book.AddOrder(MakeMarket(2, Side::Buy, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 50);
    EXPECT_EQ(book.Size(), 0);
}

TEST(MarketOrderTest, MarketSell_MatchesBestBid) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    
    auto trades = book.AddOrder(MakeMarket(2, Side::Sell, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetAskTrade().quantity_, 50);
    EXPECT_EQ(book.Size(), 0);
}

TEST(MarketOrderTest, MarketBuy_SweepsMultipleLevels) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    book.AddOrder(MakeGTC(2, Side::Sell, 101, 50));
    book.AddOrder(MakeGTC(3, Side::Sell, 102, 50));
    auto trades = book.AddOrder(MakeMarket(4, Side::Buy, 150));
    
    ASSERT_EQ(trades.size(), 3);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 50); 
    EXPECT_EQ(trades[1].GetBidTrade().quantity_, 50);  
    EXPECT_EQ(trades[2].GetBidTrade().quantity_, 50);  
    EXPECT_EQ(book.Size(), 0);
}

TEST(MarketOrderTest, MarketBuy_PartialFill_RemainderRests) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    
    auto trades = book.AddOrder(MakeMarket(2, Side::Buy, 100));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 50);
    EXPECT_EQ(book.Size(), 1);
}

TEST(MarketOrderTest, MarketBuy_NoAsks_NoBids_Rejected) {
    Orderbook book;
    auto trades = book.AddOrder(MakeMarket(1, Side::Buy, 100));
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 0);
}
