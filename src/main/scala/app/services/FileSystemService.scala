package app.services

trait FileSystemService[F[_]]:
  /**
   * Reads the entire content of a file into a String.
   *
   * @param fileName
   *   The name or path of the file to read.
   * @return
   *   An F containing the file content as a String, or an error if the file cannot be read.
   */
  def readFileContent(fileName: String): F[String]

  /**
   * Reads the content of two files in parallel and concatenates their contents.
   *
   * @param fileName1
   *   The name or path of the first file.
   * @param fileName2
   *   The name or path of the second file.
   * @return
   *   An F containing the concatenated content of the two files, or an error if either file cannot
   *   be read.
   */
  def readTwoFilesInParallel(fileName1: String, fileName2: String): F[String]
