package org.sugis.jacksonfiber;

import java.nio.file.Paths;

import org.junit.Test;

import co.paralleluniverse.fibers.instrument.SuspendablesScanner;

public class GenerateSuspendables {
    @Test
    public void generateSuspendables() {
        SuspendablesScanner scanner = new SuspendablesScanner(Paths.get("."));
        scanner.setSuspendablesFile("suspendables");
        scanner.setSupersFile("supers");
        scanner.execute();
    }
}
