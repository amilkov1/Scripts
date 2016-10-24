/**
  * Created by amilkov on 10/23/16.
  */
object Example {    

  trait Functor[F[_]] { 
    def map[A, B](a: F[A], f: A => B): F[B] 
  }    

  implicit val optFunctor: Functor[Option] = new Functor[Option] { 
    override def map[A, B](a: Option[A], f: (A) => B): Option[B] = a.map(f) 
  } 

  def fmap[F[_] : Functor, A, B](a: F[A], f: A => B): F[B] = { 
    implicitly[Functor[F]].map(a, f) 
  }    

  def fmap2[F[_], A, B](a: F[A], f: A => B)(implicit ev: Functor[F]): F[B] = { 
    ev.map(a, f) 
  }
}
