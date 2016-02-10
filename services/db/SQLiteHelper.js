/**
 * Created by Sjoerd Houben on 10/02/2016.
 */

var _ = require('underscore');
var fs = require("fs");
var file = "bitbot.db";
var exists = fs.existsSync(file);
var sqlite3 = require("sqlite3").verbose();
var db = new sqlite3.Database(file);

var SQLiteHelper = function () {
  //_.bindAll(this, 'push', 'getLastNCandles', 'getAllCandles', 'getAllCandlesSince', 'getLastClose', 'getLastNonEmptyPeriod', 'getLastNonEmptyClose', 'getLastNCompleteAggregatedCandleSticks', 'getLastCompleteAggregatedCandleStick', 'getCompleteAggregatedCandleSticks', 'getLastNAggregatedCandleSticks', 'getAggregatedCandleSticks', 'getAggregatedCandleSticksSince', 'calculateAggregatedCandleStick', 'aggregateCandleSticks', 'removeOldDBCandles', 'dropCollection', 'getInitialBalance', 'setInitialBalance');
}

SQLiteHelper.prototype.push = function (csArray, callback) {
  db.serialize(function () {
    var stmt2 = db.prepare('INSERT INTO krakenXXBTZEUR (period, open, high, low, close ,volume , vwap) VALUES (?,?,?,?,?,?,?);')
    _.forEach(csArray, function (cs) {
      console.log("Period: " + cs.period + " is being inserted in SQLite DB. With Open:" + cs.open +". High:" +cs.high + ". Low:" +cs.low
        +". Close:" +cs.close +". Volume:" +cs.volume +". VWAP:" + cs.vwap);
      stmt2.run(cs.period, cs.open, cs.high, cs.low, cs.close, cs.volume, cs.vwap, function (err) {
      });
    });
    stmt2.finalize();
  });
}

SQLiteHelper.prototype.getLastNCandles = function (N, callback) {
  db.serialize(function () {
    var candles = [];
    db.each("SELECT * FROM krakenXXBTZEUR LIMIT ?"), function (err, row) {
      var candle = {};
      candle.id = row.id;
      candle.period = row.period;
      candle.open = row.open;
      candle.high = row.high;
      candle.low = row.low;
      candle.close = row.close;
      candle.volume = row.volume;
      candle.vwap = row.vwap;
      candles.push(candle)
    }, function (err) {
      if (err) {
        callback(err, []);
      } else {
        callback(null, candles.reverse());
      }
    }
  })

}

SQLiteHelper.prototype.getAllCandles = function(callback){
  db.serialize(function(){
    var candles = [];
    db.each("SELECT * FROM krakenXXBTZEUR"), function(err,row){
      var candle ={};
      candle.id = row.id;
      candle.period = row.period;
      candle.open = row.open;
      candle.high = row.high;
      candle.low = row.low;
      candle.close = row.close;
      candle.volume = row.volume;
      candle.vwap = row.vwap;
      candles.push(candle)
    }, function (err) {
      if (err) {
        callback(err, []);
      } else {
        callback(null, candles);
      }
    }
  })
}

module.exports = SQLiteHelper;
