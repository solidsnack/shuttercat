{-# LANGUAGE OverloadedStrings
           , TupleSections
  #-}

import           Prelude hiding (lines)
import           Control.Applicative
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Monad
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn, pack)
import qualified Data.ByteString.Char8 as ByteString hiding (breakEnd, foldl')
import           Data.Monoid
import           Data.Time
import           Data.Time.Clock ()
import           Data.Word
import           System.Exit
import           System.IO
import           System.Mem

import           System.Console.CmdTheLine

import           Control.Concurrent.STM.ShutterChan


main :: IO ()
main  = run (cmd <$> style <*> millis <*> above <*> below, info)
 where cmd style millis above below
         = style (1000*millis) (ByteString.pack <$> above)
                               (ByteString.pack <$> below) stdin
       info = defTI { termName = "shuttercat", version = "(unversioned)" }

style :: Term (Int -> Maybe ByteString -> Maybe ByteString -> Handle -> IO ())
style  = value $ vFlag lines
 [(lines,  opt "nl"  "Send full lines (the default)."),
  (csv,    opt "csv" "Send full CSV records (lines, accounting for quoting)."),
  (octets, opt "raw" "Send whatever bytes are ready at each tick.")]
 where lines  = records atLastFullLine
       csv    = records csvSplit
       octets = records (,"")
       opt s desc = (optInfo [s]) {optDoc=desc, optSec="RECORD HANDLING"}

millis :: Term Int
millis  = value . opt 25 $ (optInfo [ "ms" ])
                           { optDoc  = "Cycle time in milliseconds."
                           , optName = "MILLIS" }

above :: Term (Maybe String)
above  = value . opt Nothing $ (optInfo [ "above" ])
                               { optDoc  = "Lines placed above each chunk."
                               , optName = "TEXT" }

below :: Term (Maybe String)
below  = value . opt Nothing $ (optInfo [ "below" ])
                               { optDoc  = "Lines placed below each chunk."
                               , optName = "TEXT" }


records :: (ByteString -> (ByteString, ByteString))
        -> Int -> Maybe ByteString -> Maybe ByteString -> Handle -> IO ()
records split millis above below h = do
  (segmented, batched, scrap) <- atomically ctx
  a <- async $ recv split h scrap  segmented
  b <- async $ handoff             segmented batched
  c <- async $ send above below              batched
  mapM_ wait [a, b, c]
  exitSuccess
 where ctx = (,,) <$> newTChan <*> newTChan <*> newTVar ""
       handoff = transfer millis (return ()) performGC


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr


recv :: (ByteString -> (ByteString, ByteString))
     -> Handle -> TVar ByteString
     -> TChan (Maybe ByteString) -> IO ()
recv f h v o = go
 where go = do bytes <- ByteString.hGetSome h 16384
               when (bytes /= "") . atomically $ do
                 (full, rest) <- f . (<>bytes) <$> readTVar v
                 when (full /= "") (writeTChan o (Just full))
                 writeTVar v rest
               (eof, closed) <- (,) <$> hIsEOF h <*> hIsClosed h
               if eof || closed then atomically (writeTChan o Nothing) else go

send :: Maybe ByteString -> Maybe ByteString -> TChan [ByteString] -> IO ()
send above below i = do chunks <- atomically $ readTChan i
                        (chunks /= []) `when` do
                          maybe (return ()) (ByteString.hPutStrLn h) above
                          mapM_ (ByteString.hPutStr h) chunks
                          maybe (return ()) (ByteString.hPutStrLn h) below
                          hFlush h
                          send above below i
                       where h = stdout

atLastFullLine :: ByteString -> (ByteString, ByteString)
atLastFullLine  = ByteString.breakEnd (== 0x0a)

-- Handles CSV records, which are delimited by newlines but allow quoting.
-- Thus we scan for balanced quotes.
csvSplit :: ByteString -> (ByteString, ByteString)
csvSplit b = ByteString.splitAt index b
 where (_, _, index) = ByteString.foldl' step (False, 0, 0) b

step :: (Bool, Int, Int) -> Word8 -> (Bool, Int, Int)
step (quoted, index, last) 0x22 = (not quoted, index+1, last)    -- '"'
step (False,  index, _   ) 0x0a = (False,      index+1, index+1) -- '\n'
step (quoted, index, last) _    = (quoted,     index+1, last)

