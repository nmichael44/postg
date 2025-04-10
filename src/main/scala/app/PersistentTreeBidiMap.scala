package app

import scala.collection.immutable.TreeMap

final class PersistentTreeBidiMap[K: Ordering, V: Ordering] private (
    m0: TreeMap[K, V],
    m1: TreeMap[V, K],
):
  def size: Int = m0.size

  def getWithLeft(k: K): Option[V] = m0.get(k)
  def getWithRight(v: V): Option[K] = m1.get(v)

  def updated(k: K, v: V): PersistentTreeBidiMap[K, V] =
    val m0New = m0.removed(k).updated(k, v)
    val m1New = m1.removed(v).updated(v, k)

    new PersistentTreeBidiMap[K, V](m0New, m1New)

  def removeLeftKey(k: K): PersistentTreeBidiMap[K, V] =
    m0.get(k)
      .fold(this): v =>
        val m0New = m0.removed(k)
        val m1New = m1.removed(v)

        new PersistentTreeBidiMap[K, V](m0New, m1New)

  def removeRightKey(v: V): PersistentTreeBidiMap[K, V] =
    m1.get(v)
      .fold(this): k =>
        val m0New = m0.removed(k)
        val m1New = m1.removed(v)

        new PersistentTreeBidiMap[K, V](m0New, m1New)

  def iterator: Iterator[(K, V)] = m0.iterator

  def keysLeft: Iterable[K] = m0.keys

  def keysRight: Iterable[V] = m1.keys

  def containsLeftKey(k: K): Boolean = m0.contains(k)

  def containsRightKey(v: V): Boolean = m1.contains(v)

object PersistentTreeBidiMap:
  def empty[K: Ordering, V: Ordering] =
    new PersistentTreeBidiMap[K, V](TreeMap.empty, TreeMap.empty)

  def from[K: Ordering, V: Ordering](it: IterableOnce[(K, V)]): PersistentTreeBidiMap[K, V] =
    val m = empty[K, V]

    it.iterator.foldLeft(m):
      case (mp, (k, v)) => mp.updated(k, v)
