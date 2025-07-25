package app

object ReturningThis:
  trait Pet0:
    def name: String
    def renamed(newName: String): Pet0

  case class Fish0(name: String, age: Int) extends Pet0:
    def renamed(newName: String): Fish0 = copy(name = newName)

  // F-Bounded Types
  /**
   * The base trait. It uses an F-Bound (`A <: Pet[A]`) to ensure methods can refer to the eventual concrete subtype. The
   * self-type (`this: A =>`) locks the implementation, ensuring a class cannot accidentally implement `Pet` with another
   * class's type.
   */
//trait Pet[A <: Pet[A]] {
//  this: A =>
//  def name: String
//  def renamed(newName: String): A
//}
//
//// --- A direct implementation of Pet ---
//
///**
// * Fish is a concrete class that "closes the loop" on the Pet trait directly.
// * It extends Pet[Fish], satisfying the F-Bound and self-type. The `name`
// * field in the case class constructor implements the abstract `def name` from Pet.
// */
//case class Fish(name: String, age: Int) extends Pet[Fish] {
//  override def renamed(newName: String): Fish = this.copy(name = newName)
//}
//
//// --- A deeper inheritance hierarchy ---
//
///**
// * An intermediate abstract class in the hierarchy.
// * To avoid breaking the chain of type information, it must also be generic
// * and propagate the F-Bound to its own children (`A <: Mammal[A]`).
// */
//abstract class Mammal[A <: Mammal[A]](val name: String) extends Pet[A] {
//  this: A =>
//}
//
///**
// * Monkey is a concrete class that extends the intermediate Mammal class.
// * It closes the loop by providing its own type, `Monkey`, to Mammal's
// * generic parameter, thus satisfying all constraints up the chain. It must
// * use `override val` for `name` as it's overriding a concrete `val` from Mammal.
// */
//case class Monkey(override val name: String, bananas: Int)
//  extends Mammal[Monkey](name) {
//  override def renamed(newName: String): Monkey = this.copy(name = newName)
//}

  // Instead of F-Bounds, this is the type class solution
  sealed trait Pet {
    def name: String
  }

  // Your data types extend the trait
  case class Dog(name: String, goodBoys: Int) extends Pet

  case class Cat(name: String, lives: Int) extends Pet

  trait Renamable[T]:
    def renamed(t: T, newName: String): T

  given Renamable[Dog]:
    def renamed(d: Dog, newName: String): Dog = d.copy(name = newName)

  given Renamable[Cat]:
    def renamed(c: Cat, newName: String): Cat = c.copy(name = newName)

  extension [T: Renamable as ren](t: T) {
    private def rename(newName: String): T = ren.renamed(t, newName)
  }

  // Now you can create a List[Pet]
  private val myPets: List[Pet] = List(Dog("Fido", 100), Cat("Felix", 9))

  private val myRenamedPets: List[Pet] = myPets.map {
    case d: Dog => d.rename("Mr " + d.name)
    case c: Cat => c.rename("Ms " + c.name)
  }

  println(myPets)
  println(myRenamedPets)
end ReturningThis
