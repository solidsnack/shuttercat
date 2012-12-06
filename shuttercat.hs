{-# LANGUAGE OverloadedStrings #-}

import           Control.Applicative
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Monad
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn, pack)
import qualified Data.ByteString.Char8 as ByteString hiding (breakEnd)
import           Data.Monoid
import           Data.Time
import           Data.Time.Clock ()
import           System.Exit
import           System.IO
import           System.Mem

import           Control.Concurrent.STM.ShutterChan


main :: IO ()
main  = lines 250000 stdin

lines :: Int -> Handle -> IO ()
lines t h = do (blocks, lines, chunks, scrap) <- atomically ctx
               a <- async $ recv h           blocks
               b <- async $ fullLines scrap  blocks lines
               c <- async $ handoff t               lines chunks
               d <- async $ send                          chunks
               mapM_ wait [a, b, c, d]
               exitSuccess
 where ctx = (,,,) <$> newTChan <*> newTChan <*> newTChan <*> newTVar ""

octets :: Int -> Handle -> IO ()
octets t h = do (blocks, chunks) <- atomically ctx
                a <- async $ recv h     blocks
                b <- async $ handoff t  blocks chunks
                c <- async $ send              chunks
                mapM_ wait [a, b, c]
                exitSuccess
 where ctx = (,) <$> newTChan <*> newTChan

handoff t  = transfer t now performGC
 where now = (hPutStrLn stderr . show) =<< getCurrentTime


recv :: Handle -> TChan (Maybe ByteString) -> IO ()
recv h o = do bytes <- ByteString.hGetSome h 16384
              ("" /= bytes) `when` atomically (writeTChan o (Just bytes))
              (eof, closed) <- (,) <$> hIsEOF h <*> hIsClosed h
              if eof || closed then atomically (writeTChan o Nothing)
                               else recv h o

fullLines :: TVar ByteString -> TChan (Maybe ByteString)
                             -> TChan (Maybe ByteString) -> IO ()
fullLines scrap i o = do
  continue <- atomically (maybe stop step =<< readTChan i)
  when continue (fullLines scrap i o)
 where stop = False <$ writeTChan o Nothing
       step "" = return True
       step b  = do a <- readTVar scrap
                    let (full, a') = ByteString.breakEnd (== 0x0a) (a <> b)
                    writeTChan o (Just full)
                    writeTVar scrap a'
                    return True

send :: TChan [ByteString] -> IO ()
send i = do chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr

