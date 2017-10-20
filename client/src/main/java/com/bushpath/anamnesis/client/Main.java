package com.bushpath.anamnesis.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static final String USAGE = 
        "anamnesis [OPTIONS] COMMAND\n" +
        "  OPTIONS\n" +
        "    -c --client       client name\n" +
        "    -i --ip           ip address of namenode\n" +
        "    -o --port         port of namenode\n" +
        "\n" +
        "    -b --block-size   size of blocks in file\n" +
        "    -f --favored      datanodeuuid of favored node\n" +
        "  COMMANDS\n" +
        "    download <remote> <local>    download file\n" +
        "    help                         display this screen\n" +
        "    ls <path>                    list contents (-p)\n" +
        "    mkdir <path>                 create a directory (-p)\n" +
        "    upload <local> <remote>      upload file (-b, -f)";

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

            if (arguments.command.size() < 1) {
                throw new Exception("Command not specified.");
            }

            // execute command
            switch (arguments.command.get(0)) {
            case "download":
                if (arguments.command.size() != 3) {
                    throw new Exception("Invalid arguments for command");
                }

                dfsClient.download(arguments.command.get(1), arguments.command.get(2));
                break;
            case "ls":
                if (arguments.command.size() != 2) {
                    throw new Exception("Invalid arguments for command");
                }
                
                dfsClient.ls(arguments.command.get(1));
                break;
            case "mkdir":
                if (arguments.command.size() != 2) {
                    throw new Exception("Invalid arguments for command");
                }
                
                dfsClient.mkdir(arguments.command.get(1));
                break;
            case "upload":
                if (arguments.command.size() != 3) {
                    throw new Exception("Invalid arguments for command");
                }

                dfsClient.upload(arguments.command.get(1), arguments.command.get(2), 
                    arguments.blockSize, arguments.favoredNodes);
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
        List<String> command = new ArrayList<>();

        @Parameter(names = {"-b", "--block-size"}, description = "block size")
        Integer blockSize = 64000;

        @Parameter(names = {"-f", "--favored"},
                description = "datanode uuid of favored nodes")
        List<String> favoredNodes = new ArrayList<>();
    }
}
