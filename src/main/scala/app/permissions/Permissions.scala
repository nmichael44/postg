package app.permissions

import cats.data.NonEmptyVector

import app.ImplicitConversions.*

object Permissions:
  enum Permission:
    case CanReadDirectors
    case CanReadActors
    case CanReadMovies
    case CanReadAnything

    case CanWriteDirectors
    case CanWriteActors
    case CanWriteMovies
    case CanWriteAnything

    case CanSendEmail

    case CanCreateSystemUser
    case CanFetchSystemUser
  end Permission

  private val permissionMap: Map[String, Permission] =
    Permission.values.map(p => p.toString -> p).toMap

  def fromString(s: String): Permission =
    permissionMap.getOrElse(s, throw AssertionError(s"Bad permission '$s'."))
  end fromString

  enum PermissionAlgebra:
    case Has(permission: Permission)
    case And(permissionAlgebras: NonEmptyVector[PermissionAlgebra])
    case Or(permissionAlgebras: NonEmptyVector[PermissionAlgebra])
    case Not(permissionAlgebra: PermissionAlgebra)

    def compile: CompiledPermissionAlgebra =
      this match {
        case Has(permission) => _.contains(permission)
        case And(pas) =>
          val cpas = pas.view.map(_.compile).toList
          cpas match {
            case cpa :: Nil => cpa
            case cpa0 :: cpa1 :: Nil => s => cpa0(s) && cpa1(s)
            case cpa0 :: cpa1 :: cpa2 :: Nil => s => cpa0(s) && cpa1(s) && cpa2(s)
            case _ => s => cpas.forall(_(s))
          }
        case Or(pas) =>
          val cpas = pas.view.map(_.compile).toList
          cpas match {
            case cpa :: Nil => cpa
            case cpa0 :: cpa1 :: Nil => s => cpa0(s) || cpa1(s)
            case cpa0 :: cpa1 :: cpa2 :: Nil => s => cpa0(s) || cpa1(s) || cpa2(s)
            case _ => s => cpas.exists(_(s))
          }
        case Not(pa) =>
          val cpa = pa.compile
          s => !cpa(s)
      }
    end compile
  end PermissionAlgebra

  opaque type CompiledPermissionAlgebra = Set[Permission] => Boolean

  extension (cpa: CompiledPermissionAlgebra)
    def isSatisfiedBy(userPermissions: Set[Permission]): Boolean =
      cpa(userPermissions)
    end isSatisfiedBy
end Permissions
