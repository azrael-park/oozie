package org.apache.oozie.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class DateELFunctions {

    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Indicating the day field of the month. Defined for EL as 'DAYS'.
     */
    public static final String YEARS = "YEARS";

    /**
     * Indicating the day field of the month. Defined for EL as 'DAYS'.
     */
    public static final String MONTHS = "MONTHS";

    /**
     * Indicating the day field of the month. Defined for EL as 'DAYS'.
     */
    public static final String DAYS = "DAYS";

    /**
     * Indicating the hour field of the day. Defined for EL as 'HOURS'.
     */
    public static final String HOURS = "HOURS";

    /**
     * Indicating the minute field of the hour. Defined for EL as 'MINUTES'.
     */
    public static final String MINUTES = "MINUTES";

    /**
     * Indicating the minute field of the hour. Defined for EL as 'MINUTES'.
     */
    public static final String SECONDS = "SECONDS";

    /**
     * Return formatted the current date using {@link #DEFAULT_FORMAT} which is 'yyyy-MM-dd
     * HH:mm:ss'
     *
     * @return a formatted date
     */
    public static String current() {
        return current_format(DEFAULT_FORMAT);
    }

    /**
     * Return formatted the current date using given format.
     *
     * @param format a parsing format such as 'yyyy-MM-dd HH:mm:ss'
     * @return a formatted date
     */
    public static String current_format(String format) {
        return current_by(format, TimeZone.getDefault().getID());
    }

    /**
     * Return formatted the current date using given format and timezone.
     *
     * @param format   a parsing format such as 'yyyy-MM-dd HH:mm:ss'
     * @param timezone a timezone
     * @return a formatted date
     */
    public static String current_by(String format, String timezone) {
        Calendar currentCalendar = Calendar.getInstance(TimeZone.getTimeZone(timezone));
        int truncateField = checkFormat(format);

        DateFormat formatter = new SimpleDateFormat(checkFormatString(format));
        formatter.setCalendar(currentCalendar);

        return truncateIfNecessary(formatter, currentCalendar, truncateField);
    }

    /**
     * Adds or subtracts the specified of time amount to the current time.
     * <p/>
     * <p/>
     * <code>add(DateELFunctions.MINUTES, -10)</code>.
     *
     * @param field  the calendar field which is one of the following constants :
     *               <ul>
     *               <li>{@link #YEARS} <li>{@link #MONTHS} <li>{@link #DAYS} <li>{@link #HOURS} <li>
     *               {@link #MINUTES}
     *               </ul>
     * @param amount the amount of the date or time to be added. ex> 10, -10
     * @return a formatted date
     * @throws ParseException
     */
    public static String add(String field, int amount) throws ParseException {
        return add_by_calendar(DEFAULT_FORMAT, TimeZone.getDefault().getID(),
                Calendar.getInstance(), field, amount);
    }

    /**
     * Adds or subtracts the specified of time amount to the given <code>baseDate</code>.
     * <p/>
     * <p/>
     * <code>add_basedate("2012-04-25 12:10:00",DateELFunctions.MINUTES, -10)</code>.
     *
     * @param baseDate the target time to add
     * @param field    the calendar field which is one of the following constants :
     *                 <ul>
     *                 <li>{@link #YEARS} <li>{@link #MONTHS} <li>{@link #DAYS} <li>{@link #HOURS} <li>
     *                 {@link #MINUTES}
     *                 </ul>
     * @param amount   the amount of the date or time to be added. ex> 10, -10
     * @return a formatted date
     * @throws ParseException
     */
    public static String add_basedate(String baseDate, String field, int amount)
            throws ParseException {
        DateFormat formatter = new SimpleDateFormat(checkFormatString(DEFAULT_FORMAT));
        Calendar baseCalendar = Calendar.getInstance();
        baseCalendar.setTime(formatter.parse(baseDate));
        return add_by_calendar(DEFAULT_FORMAT, TimeZone.getDefault().getID(), baseCalendar, field,
                amount);
    }

    /**
     * Adds or subtracts the specified of time amount to the current date.
     * <p/>
     * <p/>
     * <code>add_format("yyyy-MM-dd HH:mm:ss","2012-04-25 12:10:00",DateELFunctions.MINUTES, -10)</code>.
     *
     * @param format the date format. ex>yyyy-MM-dd HH:mm:ss
     * @param field  the calendar field which is one of the following constants :
     *               <ul>
     *               <li>{@link #YEARS} <li>{@link #MONTHS} <li>{@link #DAYS} <li>{@link #HOURS} <li>
     *               {@link #MINUTES}
     *               </ul>
     * @param amount the amount of the date or time to be added. ex> 10, -10
     * @return a formatted date
     * @throws ParseException
     */
    public static String add_format(String format, String field, int amount) throws ParseException {
        return add_by_calendar(format, TimeZone.getDefault().getID(), Calendar.getInstance(),
                field, amount);
    }

    /**
     * Adds or subtracts the specified of time amount to the given <code>baseDate</code>.
     * <p/>
     * <p/>
     * <code>add_format_basedate("yyyy-MM-dd HH:mm:ss","2012-04-25 12:10:00",DateELFunctions.MINUTES, -10)</code>.
     *
     * @param format   the date format. ex>yyyy-MM-dd HH:mm:ss
     * @param baseDate the target time to add
     * @param field    the calendar field which is one of the following constants :
     *                 <ul>
     *                 <li>{@link #YEARS} <li>{@link #MONTHS} <li>{@link #DAYS} <li>{@link #HOURS} <li>
     *                 {@link #MINUTES}
     *                 </ul>
     * @param amount   the amount of the date or time to be added. ex> 10, -10
     * @return a formatted date
     * @throws ParseException
     */
    public static String add_format_basedate(String format, String baseDate, String field,
                                             int amount) throws ParseException {
        DateFormat formatter = new SimpleDateFormat(checkFormatString(format));
        Calendar baseCalendar = Calendar.getInstance();
        baseCalendar.setTime(formatter.parse(baseDate));
        return add_by_calendar(format, TimeZone.getDefault().getID(), baseCalendar, field, amount);
    }

    /**
     * Adds or subtracts the specified of time amount to the given <code>baseDate</code> which is
     * the result of coordinatorEL such as <code>coord:nominal</code>.
     * <p/>
     * <code>add_coord("yyyy-MM-dd HH:mm:ss",coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd HH:mm:ss'),DateELFunctions.MINUTES, -10)</code>.
     *
     * @param format   the date format. ex>yyyy-MM-dd HH:mm:ss
     * @param baseDate the target time to add calculated by coordinatorEL.
     * @param field    the calendar field which is one of the following constants :
     *                 <ul>
     *                 <li>{@link #YEARS} <li>{@link #MONTHS} <li>{@link #DAYS} <li>{@link #HOURS} <li>
     *                 {@link #MINUTES}
     *                 </ul>
     * @param amount   the amount of the date or time to be added. ex> 10, -10
     * @return
     * @throws ParseException
     */
    public static String add_coord(String format, String baseDate, String field, int amount)
            throws ParseException {
        DateFormat formatter = new SimpleDateFormat(checkFormatString(format));
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        Calendar baseCalendar = Calendar.getInstance(TimeZone.getDefault());
        baseCalendar.setTime(formatter.parse(baseDate));

        return add_by_calendar(format, TimeZone.getDefault().getID(), baseCalendar, field, amount);

    }

    public static String add_coord_echo(String format, String baseDate, String field, int amount)
            throws ParseException {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");

        StringBuilder builder = new StringBuilder();
        builder.append("date:").append("add_coord").append('(').append(format).append(',');
        builder.append(baseDate).append(',').append(field).append(',').append(String.valueOf(amount)).append(')');

        return builder.toString();
    }

    /**
     * Adds or subtracts the specified of time amount to the given <code>baseDate</code>.
     * <p/>
     * <p/>
     * <code>add("yyyy-MM-dd HH:mm:ss","2012-04-25 12:10:00",DateELFunctions.MINUTES, -10)</code>.
     *
     * @param format   the date format. ex>yyyy-MM-dd HH:mm:ss
     * @param timezone the timezone of the date
     * @param baseDate the target time to add
     * @param field    the calendar field which is one of the following constants :
     *                 <ul>
     *                 <li>{@link #DAYS} <li>{@link #HOURS} <li>{@link #MINUTES}
     *                 </ul>
     * @param amount   the amount of the date or time to be added. ex> 10, -10
     * @return a formatted date
     * @throws ParseException
     */
    public static String add_by(String format, String timezone, String baseDate, String field,
                                int amount) throws ParseException {
        DateFormat formatter = new SimpleDateFormat(checkFormatString(format));
        Calendar baseCalendar = Calendar.getInstance(TimeZone.getTimeZone(timezone));
        baseCalendar.setTime(formatter.parse(baseDate));

        return add_by_calendar(format, timezone, baseCalendar, field, amount);
    }

    /**
     * Adds or subtracts the specified of time amount to the given <code>baseDate</code>.
     * <p/>
     * <p/>
     * <code>add("yyyy-MM-dd HH:mm:ss","2012-04-25 12:10:00",DateELFunctions.MINUTES, -10)</code>.
     *
     * @param format       the date format. ex>yyyy-MM-dd HH:mm:ss
     * @param timezone     the timezone of the date
     * @param baseCalendar the target time to add
     * @param field        the calendar field which is one of the following constants :
     *                     <ul>
     *                     <li>{@link #DAYS} <li>{@link #HOURS} <li>{@link #MINUTES}
     *                     </ul>
     * @param amount       the amount of the date or time to be added. ex> 10, -10
     * @return a formatted date
     * @throws ParseException
     */
    public static String add_by_calendar(String format, String timezone, Calendar baseCalendar,
                                         String field, int amount) throws ParseException {
        int truncateField = checkFormat(format);

        String newFormat = checkFormatString(format);

        DateFormat formatter = new SimpleDateFormat(newFormat);

        int calendarField = Calendar.DAY_OF_MONTH;
        if (field.equals(YEARS)) {
            calendarField = Calendar.YEAR;
        } else if (field.equals(MONTHS)) {
            calendarField = Calendar.MONTH;
        } else if (field.equals(DAYS)) {
            calendarField = Calendar.DAY_OF_MONTH;
        } else if (field.equals(HOURS)) {
            calendarField = Calendar.HOUR_OF_DAY;
        } else if (field.equals(MINUTES)) {
            calendarField = Calendar.MINUTE;
        } else if (field.equals(SECONDS)) {
            calendarField = Calendar.SECOND;
        }

        baseCalendar.add(calendarField, amount);
        baseCalendar.setTimeZone(TimeZone.getTimeZone(timezone));
        formatter.setCalendar(baseCalendar);

        return truncateIfNecessary(formatter, baseCalendar, truncateField);
    }

    public static String truncateIfNecessary(DateFormat formatter, Calendar targetCalendar,
                                             int truncateField) {
        formatter.setCalendar(targetCalendar);

        if (truncateField != 0) {
            if (truncateField == Calendar.YEAR) {
                targetCalendar.set(truncateField, 1);
            } else if (truncateField == Calendar.MONTH) {
                targetCalendar.set(truncateField, 0);
            } else if (truncateField == Calendar.DAY_OF_MONTH) {
                targetCalendar.set(truncateField, 1);
            } else if (truncateField == Calendar.HOUR_OF_DAY) {
                targetCalendar.set(truncateField, 0);
            }
        }

        String result = formatter.format(targetCalendar.getTime());
        if (truncateField == Calendar.MINUTE || truncateField == Calendar.SECOND) {
            result = result.substring(0, result.length() - 1) + "0";
        }
        return result;
    }

    public static int checkFormat(String format) {
        if (format.endsWith("x")) {
            int result;
            char preChar = format.charAt(format.length() - 2);
            switch (preChar) {
                case 'y':
                    result = Calendar.YEAR;
                    break;
                case 'M':
                    result = Calendar.MONTH;
                    break;
                case 'd':
                    result = Calendar.DAY_OF_MONTH;
                    break;
                case 'H':
                case 'h':
                    result = Calendar.HOUR_OF_DAY;
                    break;
                case 'm':
                    result = Calendar.MINUTE;
                    break;
                case 's':
                    result = Calendar.SECOND;
                    break;
                default:
                    result = Calendar.MINUTE;
                    break;
            }
            return result;
        }
        return 0;
    }

    public static String checkFormatString(String format) {
        if (format.endsWith("x")) {
            char preChar = format.charAt(format.length() - 2);
            return format.substring(0, format.length() - 1) + preChar;
        }
        return format;
    }
}