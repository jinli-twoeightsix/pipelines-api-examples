task copy {
  File file
  command {
    cat ${file} > file.data
    cat ${file} > file2.data
  }
  output {
    Array[File] files = glob("*.data")
  }
  runtime {
    docker: "ubuntu:latest"
  }
}

workflow w {
  File file
  call copy {
    input:
      file = file
  }
}
