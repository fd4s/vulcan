package vulcan

sealed trait Lang0

final case class Declaration[N <: Int with Singleton](n: N) extends Lang0

sealed trait Declarations extends Lang0 {
  type DoesDeclare[N0 <: Int with Singleton] <: Boolean
}

object Declarations {
  case object Nil extends Declarations
  final case class Cons[N <: Int with Singleton, T <: Declarations](n: N, t: T) extends Declarations

  type DoesDeclare[Decls <: Declarations, N0 <: Int with Singleton] = Decls match {
    case Nil.type => false
    case Cons[N0, _] => true
    case Cons[_, tl] => DoesDeclare[tl, N0]
  }

  // type Combined[Decls1 <: Declarations, Decls2 <: Declarations] <: Declarations = (Decls1, Decls2) match {
  //   case (Nil.type, decls) => decls
  //   case (Cons[n0, tl], decls) => 
  // }
}

object Test extends App {
  import Declarations._

  val d = Declaration(3)
  println(d)

  val da = Cons(3, Nil)

  val dc = Cons(3, Cons(4, Cons(3, Nil)))
  println(dc)
}