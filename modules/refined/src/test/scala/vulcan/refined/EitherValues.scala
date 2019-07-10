package vulcan.refined

trait EitherValues {
  implicit final class EitherValuesSyntax[A, B](val e: Either[A, B]) {
    def value: B = e.getOrElse(throw new NoSuchElementException(s"Expected Right was $e"))
  }
}
