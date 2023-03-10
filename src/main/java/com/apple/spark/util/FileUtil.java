package com.apple.spark.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class FileUtil {

  public static String readFileAsString(String filepath) throws IOException {
    return Files.readString(Path.of(Objects.requireNonNull(filepath)));
  }
}
