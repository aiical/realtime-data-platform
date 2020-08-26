package com.xueqiu.bigdata.util

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.retry.ExponentialBackoffRetry

class DistributeLock(lockNode: String, zkServer:String) {
  private val curatorClient = CuratorFrameworkFactory
    .builder
    .connectString(zkServer)
    .connectionTimeoutMs(30 * 1000)
    .sessionTimeoutMs(60 * 1000)
    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
    .build
  curatorClient.start()

  private val lock  = new InterProcessSemaphoreMutex(curatorClient, lockNode)


  def acquire(time: Long, unit: TimeUnit): Boolean ={
    lock.acquire(30L, TimeUnit.SECONDS)
  }

  def release(): Unit ={
    lock.release()
    curatorClient.close()
  }
}
