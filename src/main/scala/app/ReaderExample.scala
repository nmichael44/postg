package app

import cats.data.Reader

object ReaderExample:
  case class Config(c0: String, c1: Boolean, c2: Int)

  private def form1Aux(n: Int, m: Int): Int =
    n * n + m
  end form1Aux

  private def form2Aux(n: Int, m: Int): Int =
    n + m * m
  end form2Aux

  private def form(n: Int, m: Int): Reader[Config, Int] =
    Reader { config =>
      if (config.c1)
        form1Aux(n, m)
      else
        form2Aux(n, m)
    }
  end form

  private def gAux(r: Reader[Config, Int]): Reader[Config, Int] =
    r.map(x => x + 1)
  end gAux

  private def g(s: String, n: Int, m: Int): Reader[Config, Int] =
    val res = for {
      a <- form(n, m)
      b <- form(m, n)
    } yield s.length + a + b

    gAux(res)
  end g

  private def h(n: Int): Reader[Config, Int] =
    g("hi", n, 23)
  end h

  def mainAux(n: Int, m: Int, config: Config): Int =
    h(n + m).run(config)
  end mainAux
end ReaderExample
