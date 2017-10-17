package com.bushpath.anamnesis.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.logging.Logger;

public class Main {
    public static final String USAGE = 
        "anamnesis [OPTIONS] COMMAND\n" +
        "  OPTIONS\n" +
        "    -i --ip             ip address of namenode\n" +
        "    -p --port           port of namenode\n" +
        "    --path              remove path\n" +
        "  COMMANDS\n" +
        "    help                display this screen\n" +
        "    mkdir               create a directory";

    public static void main(String[] args) {
        //DFSClient dfsClient = new DFSClient("localhost", 8020);

        // parse arguments
        Arguments arguments = new Arguments();
        JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

        switch (arguments.command) {
        case "mkdir":
            System.out.println("TODO - mkdir '" + arguments.path + "'");
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
