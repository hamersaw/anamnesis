package com.bushpath.anamnesis.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Main {
    public static final String USAGE = 
        "anamnesis [OPTIONS] COMMAND\n" +
        "  OPTIONS\n" +
        "    -i --ip             ip address of namenode\n" +
        "    -p --port           port of namenode\n" +
        "    --path              remove path\n" +
        "  COMMANDS\n" +
        "    help                display this screen\n" +
        "    mkdir               create a directory (--path)";

    public static void main(String[] args) {
        // parse arguments
        Arguments arguments = new Arguments();
        JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

        // create DFSClient
        DFSClient dfsClient = new DFSClient("localhost", 8020);

        // execute command
        switch (arguments.command) {
        case "mkdir":
            dfsClient.mkdir(arguments.path);
            break;
        case "help":
        default:
            System.out.println(USAGE);
            System.exit(0);
        }
    }

    private static class Arguments {
        @Parameter(names = {"-i", "--ip"}, description = "ip address of namenode")
        String ip = "localhost";

        @Parameter(names = {"-p", "--port"}, description = "port of namenode")
        Integer port = 8020;

        @Parameter(description = "command")
        String command;

        @Parameter(names = {"--path"}, description = "path" )
        String path;
    }
}
