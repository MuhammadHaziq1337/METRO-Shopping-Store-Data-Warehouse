package src;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Logger;

public class DateUtils {
    private static final Logger logger = Logger.getLogger(DateUtils.class.getName());
    
    private static final SimpleDateFormat[] INPUT_FORMATS = new SimpleDateFormat[] {
        new SimpleDateFormat("dd/MM/yyyy HH:mm"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm"),
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    };
    
    private static final SimpleDateFormat DB_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
    
    public static String standardizeDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        
        for (SimpleDateFormat format : INPUT_FORMATS) {
            try {
                format.setLenient(false);
                java.util.Date date = format.parse(dateStr.trim());
                return DB_FORMAT.format(date);
            } catch (ParseException ignored) {
                // Try next format
            }
        }
        logger.warning("Could not parse date: " + dateStr);
        throw new IllegalArgumentException("Invalid date format: " + dateStr);
    }
}