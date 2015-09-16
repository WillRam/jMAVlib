package me.drton.jmavlib.log;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * User: ton Date: 10.06.14 Time: 12:46
 */
public class CSVLogReader implements LogReader {
    private RandomAccessFile file;
    private String[] fields;
    private Map<String, String> fieldsFormats;
    private String delimiter = ",";
    private int columnTime = -1;
    private int columnutcTime = -1;
    private long sizeUpdates = -1;
    private long sizeMicroseconds = -1;
    private long startMicroseconds = -1;
    private long utcTimeReference = -1;
    private double[][] all_data;
    private int index = 0;

    public CSVLogReader(String fileName) throws IOException, FormatErrorException {
        file = new RandomAccessFile(fileName, "r");
        readFormats();
        updateStatistics();
    }

    private void readFormats() throws IOException, FormatErrorException {
        String headerLine = file.readLine();
        if (headerLine == null) {
            throw new FormatErrorException("Empty CSV file");
        }
        fields = headerLine.split(delimiter);
        fieldsFormats = new HashMap<String, String>(fields.length);
        int count = 0;

        for (String field : fields) {
            fieldsFormats.put(field, "d");
            if (field.contains("TIME_StartTime")){
                columnTime = count;
            }
            if (field.contains("GPS_GPSTime")){
                columnutcTime = count;
            }
            count ++;
        }
        if (columnTime < 0) {
            throw new FormatErrorException("TIME_StartTime column not found");
        }
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    @Override
    public boolean seek(long seekTime) throws FormatErrorException, IOException {
        file.seek(0);
        index = 0;
        file.readLine();
        if (seekTime == 0) {
            return true;
        }
        long t = 0;
        Map<String, Object> data = new HashMap<String, Object>();
        while (t < seekTime) {
            data.clear();
            try {
                t = readUpdate(data);
            } catch (EOFException e) {
                return false;
            }
        }
        return true;
    }

    private void updateStatistics() throws IOException, FormatErrorException {
        seek(0);
        int packetsNum = 0;
        long timeStart = -1;
        long timeEnd = -1;
        while (true) {
            String line = file.readLine();
            if (line == null)
                break;
            packetsNum++;
        }
        seek(0);
        all_data = new double[packetsNum][fields.length];
        for (int i=0;i<packetsNum;i++) {
            String[] values;
            try {
                values = readLineValues();
            } catch (EOFException e) {
                break;
            }
            for (int j=0; j <values.length;j++) {
                try {
                    all_data[i][j] = Double.parseDouble(values[j]);
                } catch (NumberFormatException e) {
                    all_data[i][j] = 0.0;
                }
            }
        }
        timeStart = (long) all_data[0][columnTime];
        timeEnd = (long) all_data[packetsNum-1][columnTime];
        if (columnutcTime >= 0){
            if ( all_data[packetsNum-1][columnutcTime] > 0) {
                utcTimeReference = (long)all_data[packetsNum-1][columnutcTime] - timeEnd;
            }
        }
        startMicroseconds = timeStart;
        sizeUpdates = packetsNum;
        sizeMicroseconds = timeEnd - timeStart;
        index = 0;
    }

    @Override
    public long readUpdate(Map<String, Object> update) throws IOException, FormatErrorException {
        if (index == sizeUpdates) {
            throw new EOFException();
        }
        long t = 0;
        for (int i = 0; i < fields.length; i++) {
            if (i == columnTime) {
                t = (long) all_data[index][i];
            }
            update.put(fields[i], all_data[index][i]);
        }
        index++;
        return t;
    }

    private String[] readLineValues() throws IOException {
        String line = file.readLine();
        if (line == null) {
            throw new EOFException();
        }
        return line.split(delimiter);
    }

    @Override
    public Map<String, String> getFields() {
        return fieldsFormats;
    }

    @Override
    public String getFormat() {
        return "CSV";
    }

    @Override
    public long getSizeUpdates() {
        return sizeUpdates;
    }

    @Override
    public long getStartMicroseconds() {
        return startMicroseconds;
    }

    @Override
    public long getSizeMicroseconds() {
        return sizeMicroseconds;
    }

    @Override
    public long getUTCTimeReferenceMicroseconds() {
        return utcTimeReference;  // Not supported
    }

    @Override
    public Map<String, Object> getVersion() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> getParameters() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        CSVLogReader reader = new CSVLogReader("test.csv");
        long tStart = System.currentTimeMillis();
        Map<String, Object> data = new HashMap<String, Object>();
        while (true) {
            long t;
            data.clear();
            try {
                t = reader.readUpdate(data);
            } catch (EOFException e) {
                break;
            }
            System.out.println(t + " " + data);
        }
        long tEnd = System.currentTimeMillis();
        System.out.println(tEnd - tStart);
        reader.close();
    }
}
