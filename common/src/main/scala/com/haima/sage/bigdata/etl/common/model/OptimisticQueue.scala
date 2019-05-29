package com.haima.sage.bigdata.etl.common.model

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

class OptimisticQueue[T>: Null](sizePower: Int=0) {

  private val offerSeq: AtomicInteger = new AtomicInteger(-1)
  private val takeSeq: AtomicInteger = new AtomicInteger(-1)
  private val size: Int = 1 << sizePower
  private val mask: Int = 0x7FFFFFFF >> (31 - sizePower)
  private val ringBuffer: Array[Entry] = (1 to size).map(i => new Entry(i)).toArray

  @SuppressWarnings(Array("unchecked"))
  private def next: Entry = {
    ringBuffer(offerSeq.incrementAndGet & mask)
  }

  @throws(classOf[InterruptedException])
  def offer(holder: BarrierHolder, event: T) {
    val entry: Entry = next
    val barrier: AnyRef = holder.getBarrier
    if (!entry.enterFront(barrier)) {
      offer(holder, event)
      return
    }
    if (entry.event != null) {
      barrier synchronized {
        while (entry.event != null) {
          barrier.wait()
        }
      }
    }
    entry.publish(event)
  }

  @throws(classOf[InterruptedException])
  def take(consumer: BarrierHolder): T = {
    val entry: Entry = next
    val barrier: AnyRef = consumer.getBarrier
    if (!entry.enterBack(barrier)) return take(consumer)
    if (entry.event == null) {
      barrier synchronized {
        while (entry.event == null) {
          barrier.wait()
        }
      }
    }
     entry.take
  }

  private class Entry(id: Int=0) {
    @volatile
    var event: T = null
    private val backDoor: AtomicReference[AnyRef] = new AtomicReference[AnyRef]
    private val frontDoor: AtomicReference[AnyRef] = new AtomicReference[AnyRef]

    def publish(event: T) {
      this.event = event
      frontDoor.set(null)
      val barrier: AnyRef = backDoor.get
      if (barrier != null) {
        barrier synchronized {
          barrier.notify()
        }
      }
    }

    def enterFront(barrier: AnyRef): Boolean = {
      frontDoor.compareAndSet(null, barrier)
    }

    def enterBack(barrier: AnyRef): Boolean = {
      backDoor.compareAndSet(null, barrier)
    }

    def take: T = {
      val e: T = event
      event = null
      backDoor.set(null)
      val barrier: AnyRef = frontDoor.get
      if (barrier != null) {
        barrier synchronized {
          barrier.notify()
        }
      }
      e
    }

    def getId: Int = {
      id
    }
  }

}

trait BarrierHolder {
  def getBarrier: AnyRef
}

