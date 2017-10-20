package com.bushpath.anamnesis.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Main {
    public static final String USAGE = 
        "anamnesis [OPTIONS] COMMAND\n" +
        "  OPTIONS\n" +
        "    -c --client       client name\n" +
        "    -i --ip           ip address of namenode\n" +
        "    -o --port         port of namenode\n" +
        "\n" +
        "    -b --block-size   size of blocks in file\n" +
        "    -l --local-path   local path\n" +
        "    -p --path         remove path\n" +
        "  COMMANDS\n" +
        "    help              display this screen\n" +
        "    ls                list contents (-p)\n" +
        "    mkdir             create a directory (-p)\n" +
        "    upload            create a new file (-b, -l, -p)";

    public static void main(String[] args) {
        try {
            // parse arguments
            Arguments arguments = new Arguments();
            JCommander.newBuilder()
                .addObject(arguments)
                .build()
                .parse(args);

            // create DFSClient
            DFSClient dfsClient = new DFSClient(arguments.ip, arguments.port, 
                arguments.client);

            // execute command
            switch (arguments.command) {
            case "download":
                dfsClient.download(arguments.path, arguments.localPath);
                break;
            case "ls":
                dfsClient.ls(arguments.path);
                break;
            case "mkdir":
                dfsClient.mkdir(arguments.path);
                break;
            case "upload":
                dfsClient.upload(arguments.localPath, arguments.path, 
                    arguments.blockSize);
                break;
            case "help":
            default:
                System.out.println(USAGE);
                System.exit(0);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    private static class Arguments {
        @Parameter(names = {"-c", "--client"}, description = "client name")
        String client = "anamnesis";

        @Parameter(names = {"-i", "--ip"}, description = "ip address of namenode")
        String ip = "localhost";

        @Parameter(names = {"-o", "--port"}, description = "port of namenode")
        Integer port = 8020;

        @Parameter(description = "command")
        String command;

        @Parameter(names = {"-p", "--path"}, description = "path" )
        String path = "";

        @Parameter(names = {"-l", "--local-path"}, description = "local path" )
        String localPath = "";

        @Parameter(names = {"-b", "--block-size"}, description = "block size" )
        Integer blockSize = 64000;
    }
}
