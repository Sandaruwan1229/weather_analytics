package org.weather.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

public class WeatherJobUtils {
    private static final SimpleDateFormat FORMAT_MD_YYY = new SimpleDateFormat("MM/dd/yyyy");
    private static final SimpleDateFormat FORMAT_DM_YYYY = new SimpleDateFormat("dd/MM/yyyy");

    public static boolean isValidValue(String value) {
        return value != null && !value.trim().isEmpty() && !"null".equals(value.trim());
    }

    public static Date parseDate(String dateString)  throws ParseException {
        if (dateString == null || dateString.trim().isEmpty()) {
            throw new ParseException("date string is empty", 0);
        }

        String trimDate = dateString.trim();

        try {
            return FORMAT_MD_YYY.parse(trimDate);
        } catch (ParseException e) {
            try {
                return FORMAT_DM_YYYY.parse(trimDate);
            } catch (ParseException e1) {
                throw new ParseException("Unable to parse date:" + dateString, 0);
            }
        }
    }

    public static String getYearMonthKey(String dateString) throws ParseException {
        Date date = parseDate(dateString);
        Instant instant = date.toInstant();
        LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();

        int year = localDate.getYear();
        int month = localDate.getMonthValue(); // 1 = Jan, 12 = Dec

        return String.format("%04d-%02d", year, month);
    }
}
