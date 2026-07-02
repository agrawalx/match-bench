#include <gtest/gtest.h>
#include "orderbook.h"

static OrderPointer MakeGTC(OrderId id, Side side, Price price, Quantity qty) {
    return std::make_shared<Order>(OrderType::GoodTillCancel, id, side, price, qty);
}


TEST(OrderbookBasicTest, AddBid_EmptyBook_OrderRests) {
    Orderbook book;
    auto trades = book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 1); 
}

TEST(OrderbookBasicTest, AddAsk_EmptyBook_OrderRests) {
    Orderbook book;
    
    auto trades = book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 1);
}

TEST(OrderbookBasicTest, BidBelowAsk_NoMatch) {

    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 101, 50));
    
    auto trades = book.AddOrder(MakeGTC(2, Side::Buy, 99, 50));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 2);  
}


TEST(OrderbookBasicTest, ExactMatch_FullFill_BothSides) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    
    auto trades = book.AddOrder(MakeGTC(2, Side::Sell, 100, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().orderId_, 1);
    EXPECT_EQ(trades[0].GetBidTrade().price_, 100);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 50);
    
    EXPECT_EQ(trades[0].GetAskTrade().orderId_, 2);
    EXPECT_EQ(trades[0].GetAskTrade().price_, 100);
    EXPECT_EQ(trades[0].GetAskTrade().quantity_, 50);
    EXPECT_EQ(book.Size(), 0);
}

TEST(OrderbookBasicTest, BidPriceHigherThanAsk_StillMatches) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    
    auto trades = book.AddOrder(MakeGTC(2, Side::Buy, 105, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 50);
    EXPECT_EQ(book.Size(), 0);
}

TEST(OrderbookBasicTest, PartialFill_BidLarger) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Sell, 100, 30));
    
    auto trades = book.AddOrder(MakeGTC(2, Side::Buy, 100, 100));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().quantity_, 30);
    
    EXPECT_EQ(book.Size(), 1);
    
    auto infos = book.GetOrderInfos();
    ASSERT_EQ(infos.GetBids().size(), 1);
    EXPECT_EQ(infos.GetBids()[0].quantity_, 70);  // 100 - 30 = 70 remaining
    EXPECT_TRUE(infos.GetAsks().empty());
}

TEST(OrderbookBasicTest, PartialFill_AskLarger) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 30));
    
    auto trades = book.AddOrder(MakeGTC(2, Side::Sell, 100, 100));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetAskTrade().quantity_, 30);
    
    EXPECT_EQ(book.Size(), 1);
    auto infos = book.GetOrderInfos();
    ASSERT_EQ(infos.GetAsks().size(), 1);
    EXPECT_EQ(infos.GetAsks()[0].quantity_, 70);
}


TEST(OrderbookBasicTest, FIFO_SamePriceBids_FirstBidMatchedFirst) {
    Orderbook book;
    
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));  
    book.AddOrder(MakeGTC(2, Side::Buy, 100, 50));  
    
    auto trades = book.AddOrder(MakeGTC(3, Side::Sell, 100, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().orderId_, 1);
    
    EXPECT_EQ(book.Size(), 1);
}

TEST(OrderbookBasicTest, PricePriority_BetterPriceMatchedFirst) {
    Orderbook book;
    
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    book.AddOrder(MakeGTC(2, Side::Buy, 105, 50));  
    
    auto trades = book.AddOrder(MakeGTC(3, Side::Sell, 100, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().orderId_, 2); 
}

TEST(OrderbookBasicTest, SweepMultiplePriceLevels) {
    Orderbook book;

    book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    book.AddOrder(MakeGTC(2, Side::Sell, 101, 50));
    book.AddOrder(MakeGTC(3, Side::Sell, 102, 50));
    
    auto trades = book.AddOrder(MakeGTC(4, Side::Buy, 102, 120));

    ASSERT_EQ(trades.size(), 3);
    EXPECT_EQ(trades[0].GetAskTrade().quantity_, 50);   
    EXPECT_EQ(trades[1].GetAskTrade().quantity_, 50);   
    EXPECT_EQ(trades[2].GetAskTrade().quantity_, 20);   
    
    EXPECT_EQ(book.Size(), 1);
    auto infos = book.GetOrderInfos();
    ASSERT_EQ(infos.GetAsks().size(), 1);
    EXPECT_EQ(infos.GetAsks()[0].price_, 102);
    EXPECT_EQ(infos.GetAsks()[0].quantity_, 30);
}


TEST(OrderbookBasicTest, CancelOrder_RemovesFromBook) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    EXPECT_EQ(book.Size(), 1);
    
    book.CancelOrder(1);
    
    EXPECT_EQ(book.Size(), 0);
}

TEST(OrderbookBasicTest, CancelOrder_NonExistentId_NoOp) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    
    book.CancelOrder(999); 
    
    EXPECT_EQ(book.Size(), 1);  
}

TEST(OrderbookBasicTest, CancelOrder_PreventsMatch) {

    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    book.CancelOrder(1); 
    auto trades = book.AddOrder(MakeGTC(2, Side::Sell, 100, 50));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 1); 
}

TEST(OrderbookBasicTest, CancelOrder_CleansUpPriceLevel) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    book.CancelOrder(1);
    
    auto infos = book.GetOrderInfos();
    EXPECT_TRUE(infos.GetBids().empty()); 
}


TEST(OrderbookBasicTest, ModifyOrder_ChangesPrice) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    book.ModifyOrder(OrderModify(1, Side::Buy, 105, 50));
    
    auto infos = book.GetOrderInfos();
    ASSERT_EQ(infos.GetBids().size(), 1);
    EXPECT_EQ(infos.GetBids()[0].price_, 105); 
}

TEST(OrderbookBasicTest, ModifyOrder_ChangesQuantity) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    
    book.ModifyOrder(OrderModify(1, Side::Buy, 100, 200));
    
    auto infos = book.GetOrderInfos();
    EXPECT_EQ(infos.GetBids()[0].quantity_, 200); 
}

TEST(OrderbookBasicTest, ModifyOrder_LosesPriority) {
    Orderbook book;
    
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));  
    book.AddOrder(MakeGTC(2, Side::Buy, 100, 50)); 
    book.ModifyOrder(OrderModify(1, Side::Buy, 100, 50));
    auto trades = book.AddOrder(MakeGTC(3, Side::Sell, 100, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(trades[0].GetBidTrade().orderId_, 2); 
}

TEST(OrderbookBasicTest, ModifyOrder_NonExistentId_NoOp) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    
    auto trades = book.ModifyOrder(OrderModify(999, Side::Buy, 100, 50));
    
    EXPECT_TRUE(trades.empty());
    EXPECT_EQ(book.Size(), 1);
}

TEST(OrderbookBasicTest, ModifyOrder_CanTriggerMatch) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 99, 50)); 
    book.AddOrder(MakeGTC(2, Side::Sell, 100, 50)); 
    auto trades = book.ModifyOrder(OrderModify(1, Side::Buy, 100, 50));
    
    ASSERT_EQ(trades.size(), 1);
    EXPECT_EQ(book.Size(), 0);  
}


TEST(OrderbookBasicTest, DuplicateOrderId_Rejected) {
    Orderbook book;
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 50));
    auto trades = book.AddOrder(MakeGTC(1, Side::Sell, 100, 50));
    
    EXPECT_TRUE(trades.empty());  
    EXPECT_EQ(book.Size(), 1);   
}


TEST(OrderbookBasicTest, GetOrderInfos_BidsDescending_AsksAscending) {
    Orderbook book;
    
    book.AddOrder(MakeGTC(1, Side::Buy, 98, 50));
    book.AddOrder(MakeGTC(2, Side::Buy, 100, 50));
    book.AddOrder(MakeGTC(3, Side::Buy, 99, 50));
    book.AddOrder(MakeGTC(4, Side::Sell, 103, 50));
    book.AddOrder(MakeGTC(5, Side::Sell, 101, 50));
    book.AddOrder(MakeGTC(6, Side::Sell, 102, 50));
    
    auto infos = book.GetOrderInfos();
    ASSERT_EQ(infos.GetBids().size(), 3);
    EXPECT_EQ(infos.GetBids()[0].price_, 100);  
    EXPECT_EQ(infos.GetBids()[1].price_, 99);
    EXPECT_EQ(infos.GetBids()[2].price_, 98);
    
    ASSERT_EQ(infos.GetAsks().size(), 3);
    EXPECT_EQ(infos.GetAsks()[0].price_, 101); 
    EXPECT_EQ(infos.GetAsks()[1].price_, 102);
    EXPECT_EQ(infos.GetAsks()[2].price_, 103);
}

TEST(OrderbookBasicTest, GetOrderInfos_AggregatesQuantityAtSamePrice) {
    Orderbook book;
    
    book.AddOrder(MakeGTC(1, Side::Buy, 100, 30));
    book.AddOrder(MakeGTC(2, Side::Buy, 100, 70));
    
    auto infos = book.GetOrderInfos();
    
    ASSERT_EQ(infos.GetBids().size(), 1); 
    EXPECT_EQ(infos.GetBids()[0].price_, 100);
    EXPECT_EQ(infos.GetBids()[0].quantity_, 100); 
}
