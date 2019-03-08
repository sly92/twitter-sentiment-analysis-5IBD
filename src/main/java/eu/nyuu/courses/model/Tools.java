package eu.nyuu.courses.model;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Tools {

    public void deleteFolder(String directory_path) {

     try
    {
        FileUtils.deleteDirectory(new File(directory_path));
    } catch(
    IOException e)

    {
        e.printStackTrace();
    }
}
}
