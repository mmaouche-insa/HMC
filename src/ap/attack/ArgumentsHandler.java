/*
 * Copyright LIRIS-CNRS (2017)
 * Contributors: Mohamed Maouche  <mohamed.maouchet@liris.cnrs.fr>
 *
 * This software is a computer program whose purpose is to study location privacy.
 *
 * This software is governed by the CeCILL-B license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-B
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-B license and that you accept its terms.
 */
package ap.attack;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author mmaouche
 */
public class ArgumentsHandler {

    private String[][] optionValues = {
        {"o", "output", "path"},
        {"kd", "known-data", "path"},
        {"ud", "unknown-data", "path"},
        {"d", "dataset", "path"},
        {"ups", "ud-proportion-start", "UD proportion start 0 < value < 1"},
        {"upe", "ud-proportion-end", "UD proportion end 0 < value < 1"},
        {"kps", "kd-proportion-start", "KD proportion start 0 < value < 1"},
        {"kpe", "kd-proportion-end", "KD proportion end 0 < value < 1"},
        {"c", "cell-size", "Heat-Map's cell Size of HMC (default 800meters)"},
        {"cap", "cell-size-ap", "Heat-Map's of AP-Attack (default 800meters)"},
        {"cac", "cell-size-ac", "Area Coverage's Cell Size(default 800meters)"},
        {"di", "diameter", "POI diameter, value (meters)"},
        {"dt", "duration", "POI minimal duration,value (minutes)"},};
    private String workdir = "";
    private String kd = "";
    private String ud = "";
    private String dataset = "";
    private String ups = "0.5";
    private String upe = "1";
    private String kps = "0";
    private String kpe = "0.5";
    private String cellSize = "";
    private String cellSizeAP = "";
    private String cellSizeAC = "";
    private String diameter = "";
    private String duration = "";
    private String[] args = {""};
    private Boolean full = true;
    private Boolean hmc = false;
    private Boolean attack = false;
    private Boolean ac = false;
    private Boolean sd = false;

    public ArgumentsHandler(String[] args) {
        this.args = args;
    }

    public void addOption(Options options, String name, String longOpt, String argName) {
        options.addOption(Option.builder(name)
                .longOpt(longOpt)
                .argName(argName)
                .hasArg()
                .valueSeparator()
                .build());
    }

    public void parseArguments(String[] args) {
        Options options = createOptions();
        HelpFormatter formatter = new HelpFormatter();
        
        CommandLineParser parser = new DefaultParser();
        if (args.length == 1 && args[0] == "-h") {
            //formatter.printHelp("HMC toolkit", options);
            this.printHelp(options);
            System.exit(0);
        }
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("full")) {
                full = true;
            }
            if (line.hasOption("hmc")) {
                hmc = true;
            }
            if (line.hasOption("atk")) {
                attack = true;
            }
            if (line.hasOption("ac")) {
                ac = true;
            }
            if (line.hasOption("sd")) {
                sd = true;
            }
            if (line.hasOption("o")) {
                workdir = line.getOptionValue("o");
            }
            if (line.hasOption("kd")) {
                kd = line.getOptionValue("kd");
            }
            if (line.hasOption("ud")) {
                ud = line.getOptionValue("ud");
            }
            if (line.hasOption("d")) {
                dataset = line.getOptionValue("d");
            }
            if (line.hasOption("ups")) {
                ups = line.getOptionValue("ups");
            }
            if (line.hasOption("upe")) {
                upe = line.getOptionValue("upe");
            }
            if (line.hasOption("kps")) {
                kps = line.getOptionValue("kps");
            }
            if (line.hasOption("kpe")) {
                kpe = line.getOptionValue("kpe");
            }
            if (line.hasOption("c")) {
                cellSize = line.getOptionValue("c");
            }
            if (line.hasOption("cap")) {
                cellSize = line.getOptionValue("cap");
            }
            if (line.hasOption("cac")) {
                cellSize = line.getOptionValue("cac");
            }
            if (line.hasOption("di")) {
                diameter = line.getOptionValue("d");
            }
            if (line.hasOption("dt")) {
                duration = line.getOptionValue("t");
            }
            if (line.hasOption("h")) {
                //formatter.printHelp("HMC toolkit", options);
                this.printHelp(options);
                System.exit(0);
            }
        } catch (ParseException exp) {
            System.err.println(exp.getMessage());
            //formatter.printHelp("HMC toolkit", options);
            printHelp(options);
            System.exit(1);
        }
        if (!full && (boolToInt(hmc) + boolToInt(attack) + boolToInt(ac) + boolToInt(sd) > 1)) {
            System.out.println("Error : only one option of -hmc/-atk/-ac/-sd  can be used at time. If you want to execute multiple task use -full");
            System.exit(1);
        }
    }

    private int boolToInt(Boolean b) {
        return b ? 1 : 0;
    }

    public void parseArguments() {
        this.parseArguments(this.args);
    }

    public Options createOptions() {
        // create Options object
        Options options = new Options();
        for (String[] value : optionValues) {
            addOption(options, value[0], value[1], value[2]);
        }
        // help
        options.addOption(Option.builder("h")
                .longOpt("help")
                .argName("print help")
                .build());
        // mode 
        options.addOption(Option.builder("full")
                .longOpt("full")
                .argName("use all the mode of execution. HMC -> ATK+Utility")
                .build());
        options.addOption(Option.builder("hmc")
                .longOpt("hmc")
                .argName("Apply HMC on the dataset")
                .build());
        options.addOption(Option.builder("atk")
                .longOpt("atk")
                .argName("Apply the re-identification attacks on the train and test datasets")
                .build());
        options.addOption(Option.builder("ac")
                .longOpt("ac")
                .argName("Apply Area Coverage on the reference and obfuscated dataset")
                .build());
        options.addOption(Option.builder("sd")
                .longOpt("sd")
                .argName("Apply Spatial Distortion on the reference and obfuscated dataset")
                .build());
        return options;
    }

    public String getWorkdir() {
        return workdir;
    }

    public String getKd() {
        return kd;
    }

    public String getUd() {
        return ud;
    }

    public String getDataset() {
        return dataset;
    }

    public String getProportion(String proportion, String name) {
        if (proportion.isEmpty()) {
            return "";
        }
        try {
            double d = Double.parseDouble(proportion);
            if (d < 0 || d > 1) {
                System.out.println("Error : All proportions must be between 0 and 1, for -" + name + ", got : " + proportion);
                System.exit(0);
            }
        } catch (NumberFormatException e) {
            System.out.println("Error : Wrong number format for -" + name + ", got : " + proportion);
            System.exit(0);
        }
        return proportion;
    }

    public String getUps() {
        return getProportion(ups, "ups");
    }

    public String getUpe() {
        return getProportion(upe, "upe");
    }

    public String getKps() {
        return getProportion(kps, "kps");
    }

    public String getKpe() {
        return getProportion(kpe, "kpe");
    }

    public String getCellSize() {
        if (cellSize.isEmpty()) {
            return "";
        }
        try {
            double d = Double.parseDouble(cellSize);
            if (d < 0) {
                System.out.println("Error : -c/--cell-size must be positive, got : " + cellSize);
                System.exit(0);
            }
        } catch (NumberFormatException e) {
            System.out.println("Error : Wrong number format for -c/--cell-size, got : " + cellSize);
            System.exit(0);
        }
        return cellSize + ".meters";
    }

    public String getCellSizeAC() {
        if (cellSizeAC.isEmpty()) {
            return "";
        }
        try {
            double d = Double.parseDouble(cellSizeAC);
            if (d < 0) {
                System.out.println("Error : -cac/--cell-size-ac must be positive, got : " + cellSizeAC);
                System.exit(0);
            }
        } catch (NumberFormatException e) {
            System.out.println("Error : Wrong number format for -cac/--cell-size-ac, got : " + cellSizeAC);
            System.exit(0);
        }
        return cellSizeAC + ".meters";
    }

    public String getCellSizeAP() {
        if (cellSizeAP.isEmpty()) {
            return "";
        }
        try {
            double d = Double.parseDouble(cellSizeAP);
            if (d < 0) {
                System.out.println("Error : -cap/--cell-size-ap must be positive, got : " + cellSizeAP);
                System.exit(0);
            }
        } catch (NumberFormatException e) {
            System.out.println("Error : Wrong number format for -cap/--cell-size-ap, got : " + cellSizeAP);
            System.exit(0);
        }
        return cellSizeAP + ".meters";
    }

    public String getDiameter() {
        if (diameter.isEmpty()) {
            return "";
        }
        try {
            double d = Double.parseDouble(diameter);
            if (d < 0) {
                System.out.println("Error : -di/--diameter must be positive, got : " + diameter);
                System.exit(0);
            }

        } catch (NumberFormatException e) {
            System.out.println("Error : Wrong number format for -di/--diameter, got : " + diameter);
            System.exit(0);
        }
        return diameter + ".meters";
    }

    public String getDuration() {
        if (duration.isEmpty()) {
            return "";
        }
        try {
            double d = Double.parseDouble(duration);
            if (d < 0) {
                System.out.println("Error : -dt/--time must be positive, got : " + duration);
                System.exit(0);
            }

        } catch (NumberFormatException e) {
            System.out.println("Error : Wrong number format for -dt/--time, got : " + duration);
            System.exit(0);
        }
        return duration + ".minutes";
    }

    public ArrayList<String> getCommandLine() throws Exception {
        String json = "";
        String params = "";
        ArrayList<String> cmds = new ArrayList<>();
        cmds.add("run");
        if (!workdir.isEmpty()) {
            cmds.addAll(Arrays.asList("-workdir", workdir));
        }
        if (full) {
            json += "ressources/full.json";

            if (!dataset.isEmpty()) {
                params += "url=" + dataset;
            } else {
                throw new Exception("At least one dataset has to entered with -d path");
            }
        } else if (hmc) {
            if (ud.isEmpty()) {
                // System.out.println("one Source JSON");
                json += "ressources/hmc-one_source.json";
                if (!dataset.isEmpty()) {
                    params += "url=" + dataset;
                } else if (!kd.isEmpty()) {
                    params += "url=" + kd;
                } else {
                    throw new Exception("At least one dataset has to entered with -d path");
                }
            } else {
                json += "ressources/hmc-two_source.json";
                params = " urlUD=" + ud;
                if (!kd.isEmpty()) {
                    params += " urlKD=" + kd;
                } else if (!dataset.isEmpty()) {
                    params += " urlKD=" + dataset;
                } else {
                    throw new Exception("-ud option given but -kd was not informed");
                }
            }
        } else {
            if (attack) {
                json += "ressources/attack.json";
            } else if (ac) {
                json += "ressources/ac.json";
            } else if (sd) {
                json += "ressources/sd.json";
            }
            if (ud.isEmpty() || (kd.isEmpty() && dataset.isEmpty())) {
                throw new Exception("-kd and -ud need to be given (see help with -h)");
            }
            params = " urlUD=" + ud;
            if (!kd.isEmpty()) {
                params += " urlKD=" + kd;
            } else if (!dataset.isEmpty()) {
                params += " urlKD=" + dataset;
            } else {
                throw new Exception("-ud option given but -kd was not informed");
            }
        }

        if (!kps.isEmpty()) {
            params += " startTrain=" + getKps();
        }
        if (!kpe.isEmpty()) {
            params += " endTrain=" + getKpe();
        }
        if (!ups.isEmpty()) {
            params += " startTest=" + getUps();
        }
        if (!upe.isEmpty()) {
            params += " endTest=" + getUpe();
        }

        if (!cellSize.isEmpty()) {
            params += " cellSize=" + getCellSize();
        }
        if (!cellSizeAC.isEmpty()) {
            params += " cellSizeAC=" + getCellSizeAC();
        }
        if (!cellSizeAP.isEmpty()) {
            params += " cellSizeAP=" + getCellSizeAP();
        }
        if (!diameter.isEmpty()) {
            params += " diameter=" + getDiameter();
        }
        if (!duration.isEmpty()) {
            params += " duration=" + getDiameter();
        }
        cmds.add("-params");
        cmds.add(params);
        cmds.add(json);

        return cmds;
    }

private static void printHelp(Options opts) {
	HelpFormatter formatter = new HelpFormatter();
	formatter.setDescPadding(0);
	String header = "\n Welcome to HMC toolkit.\n  A toolkit to test the Location Privacy Protection Mechanism HMC (Heat-Map Confusion)."
                +"\n With this toolkit you can launch HMC, test re-identiication attacks and Utility metrics.";
	String footer = "\n Simple default value Usage : \n java -jar hmc.jar -d <Dataset directory> -o <output>";
	formatter.printHelp("java -jar hmc.jar", header, opts, footer, true);
}
 
}

