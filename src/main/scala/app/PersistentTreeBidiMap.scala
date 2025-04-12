package app

import scala.collection.immutable.{TreeMap, TreeSet}

final class PersistentTreeBidiMap[K: Ordering, V: Ordering] private (
    m0: TreeMap[K, V],
    m1: TreeMap[V, K],
):
  def size: Int = m0.size

  def isEmpty: Boolean = m0.isEmpty

  def getWithLeft(k: K): Option[V] = m0.get(k)
  def getWithRight(v: V): Option[K] = m1.get(v)

  def getWithLeftOrElse(k: K, v: => V): V = m0.getOrElse(k, v)
  def getWithRightOrElse(v: V, k: => K): K = m1.getOrElse(v, k)

  def updated(k: K, v: V): PersistentTreeBidiMap[K, V] =
    new PersistentTreeBidiMap[K, V](m0.removed(k).updated(k, v), m1.removed(v).updated(v, k))

  def removeLeftKey(k: K): PersistentTreeBidiMap[K, V] =
    m0.get(k).fold(this) { v =>
      new PersistentTreeBidiMap[K, V](m0.removed(k), m1.removed(v))
    }

  def removeRightKey(v: V): PersistentTreeBidiMap[K, V] =
    m1.get(v).fold(this) { k =>
      new PersistentTreeBidiMap[K, V](m0.removed(k), m1.removed(v))
    }

  def iterator: Iterator[(K, V)] = m0.iterator

  def keysLeft: Iterable[K] = m0.keys
  def keysRight: Iterable[V] = m1.keys

  def containsLeftKey(k: K): Boolean = m0.contains(k)
  def containsRightKey(v: V): Boolean = m1.contains(v)

  def keySetLeft: TreeSet[K] = m0.keySet
  def keySetRight: TreeSet[V] = m1.keySet

object PersistentTreeBidiMap:
  def empty[K: Ordering, V: Ordering] =
    new PersistentTreeBidiMap[K, V](TreeMap.empty, TreeMap.empty)

  def from[K: Ordering, V: Ordering](it: IterableOnce[(K, V)]): PersistentTreeBidiMap[K, V] =
    it.iterator.foldLeft(empty[K, V]) { case (mp, (k, v)) => mp.updated(k, v) }
