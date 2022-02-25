
package readfileindirectory;

package readfileindirectory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
//import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Optional;

public class Watcher {

    private final static PathMatcher TXTMATCHER = FileSystems.getDefault().getPathMatcher("glob:*.txt");
    private final WatchService watcher;
    private final HashMap<WatchKey, Path> keys;
    private boolean trace = false;

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    public Watcher(Path dir) throws IOException {
        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<>();
        register(dir);
        this.trace = true;
    }

    /**
     * Register the given directory with the WatchService
     */
    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_MODIFY);//o ENTRY_CREATE
        if (trace) {
            Path prev = keys.get(key);
            if (prev == null) {
                System.out.format("register: %s\n", dir);
            } else {
                if (!dir.equals(prev)) {
                    System.out.format("update: %s -> %s\n", prev, dir);
                }
            }
        }
        keys.put(key, dir);
    }

    /**
     * Process all events for keys queued to the watcher
     */
    void processEvents() {
        while (true) {
            //esperar a un evento de "archivo creado, modificado"
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("Folder no Reconocido!!");
                continue;
            }

            key.pollEvents().forEach((event) -> {
                WatchEvent.Kind kind = event.kind();
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);
                System.out.format("%s: %s\n", event.kind().name(), child);
                if (TXTMATCHER.matches(name)) {
                    procesartxt(child);
                }
            });

            // reset la llave de watcher a manera de asegurar si aun es necesario esperar eventos dentro del folder
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);
                if (keys.isEmpty()) {
                    // si ya no hay folders a escuchar salir del loop/thread
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        //leer los archivos que YA estan en el folder
        Path dir = Paths.get("D:\\Worwspace\\Trainning\\Praticas\\ReadFileInDirectory\\ReadFileInDirectory\\directorio");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.txt")) {
            stream.forEach(txtpath -> {
                procesartxt(txtpath);
            });
        }
        //Logear un visor espera de que se "edite o agregen datos al folder"
        new Watcher(dir).processEvents();
    }

    private static void procesartxt(Path child) {
        CsvParser parser = new CsvParser(child, Optional.empty());
        LinkedList<LinkedList<String>> csvdata = parser.Parse();
        if (csvdata != null) {
            csvdata.forEach(a -> {
                a.forEach(b -> {
                    System.out.printf("\"%s\",", b);
                });
                System.out.println();
            });
        }
    }
}
