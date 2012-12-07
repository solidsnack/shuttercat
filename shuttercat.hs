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

import           Control.Concurrent.STM.ShutterChan


main :: IO ()
main  = csv 25000 stdin
 where lines  = records atLastFullLine
       csv    = records csvSplit
       octets = records (,"")

records :: (ByteString -> (ByteString, ByteString)) -> Int -> Handle -> IO ()
records f t h = do (segmented, batched, scrap) <- atomically ctx
                   a <- async $ recv f h scrap  segmented
                   b <- async $ handoff t       segmented batched
                   c <- async $ send                      batched
                   mapM_ wait [a, b, c]
                   exitSuccess
 where ctx = (,,) <$> newTChan <*> newTChan <*> newTVar ""

handoff t  = transfer t now performGC
 where now = (hPutStrLn stderr . show) =<< getCurrentTime

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

send :: TChan [ByteString] -> IO ()
send i = do chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


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

