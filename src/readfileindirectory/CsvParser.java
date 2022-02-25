/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package readfileindirectory;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CvsParser. a CSV File parser compliance with
 * <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>
 *
 * @see <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>
 * @author Eduardo
 */
public class CsvParser {

    /**
     * The CSV Separator as defined by RFC4180
     */
    public static final char SEPARATOR = (char) 0x2C;
    /**
     * Line limiter(s) for File as per definition on RFC4180 RFC4180 DEFINES
     * that the rows NEEDS to be separated by CRLF HOWEVER this is not followed
     * by all implementations some might use just CR others might only use LF,
     * we will warn if only one is present but will try our best to use either.
     */
    public static final char CR = '\r', LF = '\n';
    /**
     * the escape character as define in RFC4180
     */
    public static final char ESCAPE_CHR = '"';
    /**
     * Path to Read/write the Files.
     */
    private final Path ReadPath;
    /**
     * Character Set Used to Decode the File Characters from Raw Bytes this is
     * required to ensure the right Character is printed or "understood" by Java
     * UTF-16 characters if none is provided we will use the Java Default set on
     * this VM (likely "UTF-8" or "UTF-16")
     */
    private final Charset FileCharset;

    /**
     * Initialize the CvsParser instance using the
     *
     * @param Read Path to the csv file to be read
     * @param FileCharset the Optional Parameter to set for the Character Set
     * used to decode the file into Java Characters
     */
    public CsvParser(final Path Read, Optional<Charset> FileCharset) {
        //check non null 
        this.ReadPath = Objects.requireNonNull(Read, "Read Path is Null! please provide a valid Path");
        //check we can read from Read File 
        if (!Files.isReadable(ReadPath)) {
            Logger.getLogger(CsvParser.class.getName()).log(Level.WARNING, "Warning File does not exist, Permissions are missing, or is not Readable ");
        }
        if (FileCharset == null) {
            //wtf! this is just lazy! 
            FileCharset = Optional.empty();
        }
        this.FileCharset = FileCharset.orElse(Charset.defaultCharset());
    }

    /**
     * Parse the Text File using CSV format into a grid(a LinkedList of rows)
     * the LinkedList is of "rows"(a grid), each row is represented by a
     * LinkedList of Strings
     *
     * @return a grid(LinkedList of Rows)
     */
    public LinkedList<LinkedList<String>> Parse() {
        LinkedList<LinkedList<String>> grid = null;
        //try with resourse 
        try (BufferedReader br = Files.newBufferedReader(ReadPath, FileCharset)) {
            grid = new LinkedList<>();
            StringBuilder buffer = new StringBuilder();
            boolean escaped = false;
            int read;
            while ((read = br.read()) >= 0) {
                char readchr = (char) read;
                //lets see what kind if Character it is. 
                switch (readchr) {
                    case ESCAPE_CHR:
                        escaped = parseEscape(escaped, buffer, br, readchr);
                        break;
                    case SEPARATOR:
                        parseSeparator(escaped, grid, buffer, readchr);
                        break;
                    /*here we check for the "row" separator... 
                      RFC4180 DEFINES that the Row separator SHOULD have CR followed by LF
                      however there are CSV that use only one of them either CR or LF
                      and some are formal and use both. 
                      for better support lets asume we have a file that is a colash of separators 
                      HOWEVER we will warn about this on the logs
                     */
                    case CR:
                    case LF:
                        ParseLine(escaped, grid, buffer, br, readchr);
                        break;
                    //for any other character just feed it into the buffer.
                    default:
                        buffer.append(readchr);
                        break;
                }

            }
            //while finish, however there might be uncommited data
            //on the buffer that needs to go into the csvdata 
            if (buffer.length() > 0) {
                commitBuffer(grid, buffer);
                buffer = null;
            }
        } catch (IOException ex) {
            Logger.getLogger(CsvParser.class.getName()).log(Level.SEVERE, "Error Reading The CSV File.", ex);
        }
        //try with resource will close the Stream on its own. no need for finally statement. 
        return grid;
    }

    /**
     * this method is to be called when a Escape is detected, this method will
     * review if the escape is set. if it is set it will then check if the
     * escape character is on itself "escape" so it should be handled as literal
     * and just be added into the buffer (and ignore the duplicate value as it
     * is intended to escape.) as per
     * <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>
     * definition.
     *
     * @param escape the status of the Escape flag, if set to true will check if
     * the escape character is followed by another escape character and handle
     * it as a literal adding it to the buffer. otherwise setting and returning
     * the flag as false;<\br>
     * if the flag status is False it will set as true until another escape is
     * detected.
     * @param buffer the buffer to add the Characters into. if a flag is to be
     * handled as literal it will be appended to this buffer.
     * @param br the bufferedReader to read Characters from needed to read the
     * next character to determine if is a end escaping or to be handle as
     * literal
     * @param readchr the read character (a escape character can be inferred)
     * @return the Flag new status This should be used as escape is passed as
     * value not as a reference.
     * @throws IOException if an Exception is detected upon read the buffer.
     */
    private boolean parseEscape(boolean escape, StringBuilder buffer,
            BufferedReader br, char readchr) throws IOException {
        //it is a escape Charater, if we are Alredy escaping lets
        //handle if it is a quotation 
        if (escape) {
            //lets set a bookmark to return if we need to undo. 
            br.mark(2);
            char nextchar = (char) br.read();
            if (nextchar == readchr) {
                //this is a quote character on the field. so its not to finish the "escaping"
                //so we merge this 2 character to a single "double quote"
                buffer.append(readchr);
            } else {
                //this character is not a 2DQUOTE thefore this ends the escaping
                escape = false;
                //lets go back to the bookmark we did as we need to process that nextchar on the next loop
                br.reset();
            }
        } else {
            //we are not yet escaping, so since we found a ESCAPECHAR lets begin
            escape = true;
        }
        return escape;
    }

    /**
     * Commits the Data from the StringBuilder buffer into the Grid. this is
     * done in the following manner if the grid is empty (no rows on the grid) a
     * new Row(LinkedList<String>) will be added. next it will take the Last row
     * from the Grid and add the buffer data into the Row. next will set the
     * buffer length to 0 (to clean the buffer in a computation efficient
     * manner)
     *
     * @param grid a LinkedList<LinkedList<String>> that represents the Grid of
     * data.
     * @param buff a StringBuilder that holds the buffer (read data from the
     * file to be committed into the grid)
     */
    private void commitBuffer(LinkedList<LinkedList<String>> grid, StringBuilder buff) {
        if (grid.isEmpty()) {
            //if the grid is empty we need to add the first row and start filling its content 
            grid.add(new LinkedList<>());
        }
        //get the "last known row" and add a Cell(the String data) to the end of the Row
        grid.getLast().add(buff.toString());
        //for perforance, lets "clean the lenght of the buffer"
        //this is not "secure", but is quite efficient 
        buff.setLength(0);
        //if the data needs to be nulled "secure" but slow use this line
        //buff.trimToSize(); //clear the underline Array
    }

    /**
     * this method is to be called when a Separator is detected, this method
     * will review if the escape is set. if it is set it will add the separator
     * as a character into the buffer as per defined on the standard
     * <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>
     * definition. otherwise it will mean that the Cell is completed and
     * therefore needs to commit the Buffer into the grid. by calling
     * commitBuffer(grid, buffer);
     *
     * @param escape the status of the Escape flag, if true will add the
     * character into the buffer otherwise commit the data.
     * @param grid a LinkedList<LinkedList<String>> that represents the Grid of
     * data.
     * @param buff a StringBuilder that holds the buffer (read data from the
     * file to be committed into the grid)
     * @param readchr the character read. can be inferred it is a SEPARATOR
     */
    private void parseSeparator(boolean escape, LinkedList<LinkedList<String>> grid, StringBuilder buffer, char readchr) {
        //its a SEPARATOR HOWEVER is it escaped?
        if (escape) {
            //if escaped just add into the buffer
            buffer.append(readchr);
        } else {
            //not escaped so this is a SEPARATOR then lets flush the text buffer
            //into the Row data as a new "cell" 
            commitBuffer(grid, buffer);
        }
    }

    /**
     *
     * @param escape the status of the Escape flag, if true will add the
     * character into the buffer otherwise commit the data. and add a new Row to
     * be filled
     * @param grid a LinkedList<LinkedList<String>> that represents the Grid of
     * data.
     * @param buff a StringBuilder that holds the buffer (read data from the
     * file to be committed into the grid)
     * @param br the bufferedReader to read Characters from needed to read the
     * next character to determine and consume the next character if the Line is
     * correctly terminated.
     * @param readchr the Read character from BufferedReader, this can be either
     * if the characters that consist of a Line limiter (CR or LF)
     * @throws IOException if an Exception is detected upon read the buffer.
     */
    private void ParseLine(boolean escape, LinkedList<LinkedList<String>> grid,
            StringBuilder buffer, BufferedReader br, char readchr) throws IOException {
        //its a Carriage Return or a Line Feed
        if (escape) {
            //if escaped just add into the buffer
            buffer.append(readchr);
        } else {
            //not escaped so this should be CR or LF (or CR followed by LF)
            if (readchr == CR) {
                br.mark(2);
                char nextchar = (char) br.read();
                if (nextchar != LF) {
                    Logger.getLogger(CsvParser.class.getName()).log(Level.WARNING,
                            "WARNING there is a Carriage Return but is not Followed by Line Feed!");
                    br.reset();
                }
            } else {
                Logger.getLogger(CsvParser.class.getName()).log(Level.WARNING,
                        "WARNING there is a Line Feed but is not Preceded by Carriage Return!");
            }
            commitBuffer(grid, buffer);
            grid.add(new LinkedList<>());
        }
    }
}