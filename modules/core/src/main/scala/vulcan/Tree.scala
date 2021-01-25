package vulcan

import scala.compiletime.ops.int._
import scala.compiletime.ops.boolean._

sealed abstract class Tree

case object Leaf extends Tree

final case class Branch[N <: Int & Singleton, L <: Tree, R <: Tree](n: N, l: L, r: R)(implicit ev: RightOf[N, L]) extends Tree

sealed abstract class RightOf[N <: Int & Singleton, T <: Tree]
object RightOf {
  implicit def instance[N <: Int & Singleton, T <: Tree](implicit ev: (N IsRightOf T) =:= true): RightOf[N, T] = new RightOf[N, T] {}
}

type IsRightOf[N <: Int & Singleton, T <: Tree] <: Boolean = T match {
  case Leaf.type => true
  case Branch[n, _, r] => N > n && IsRightOf[N, r]
}


type LessThan[N0 <: Int & Singleton, N1 <: Int & Singleton] = N0 < N1

sealed abstract class OrderedList
case object Nil extends OrderedList
final case class Cons[N <: Int & Singleton, Tl <: OrderedList](hd: N, tl: Tl)(implicit ev: (N NotIn Tl) =:= true) extends OrderedList

type NotIn[N <: Int & Singleton, OL <: OrderedList] <: Boolean = OL match {
  case Nil.type => true
  case Cons[n, tl] => (N > n || N < n) && (N NotIn tl)
}

import scala.compiletime.error

type GreaterNumber[N <: Int & Singleton, N0 <: Int & Singleton] <: Int = (N > N0) match {
  case true => N
}

final case class Ordered[N0 <: Int & Singleton, N1 <: Int & GreaterNumber[N1, N0]](n0: N0, n1: N1)

// final case class Ordered[N0 <: Int & Singleton, N1 <: Int & Singleton](n0: N0, n1: N1)(implicit ev: (N0 LessThan N1) =:= true)

object LeafTest extends App {
 val foo: 3 + 4 = 7
 val bar: true = 3 < 4

 val tree = Branch[1, Branch[0, Leaf.type, Leaf.type], Leaf.type](1, Branch(0, Leaf, Leaf), Leaf)
 //val tree2 = Branch(1, Branch(0, Leaf, Leaf), Leaf)

 Cons(0, Cons(1, Nil))

 //Ordered(0, 1)

 val baz: GreaterNumber[2, 1] = 2
}
