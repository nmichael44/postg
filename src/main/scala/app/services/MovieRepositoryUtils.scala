package app.services

object MovieRepositoryUtils:
  enum DBError:
    case DuplicateLoginName(loginName: String)
  end DBError
end MovieRepositoryUtils
