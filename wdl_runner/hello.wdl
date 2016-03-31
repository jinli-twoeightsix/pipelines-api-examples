task hello {
  File file
  command {
    cat ${file}
  }
  output {
    String contents = read_string(stdout())
  }
  runtime {
    docker: "ubuntu:latest"
  }
}

workflow w {
  File file
  call hello {
    input:
      file = file
  }
}
