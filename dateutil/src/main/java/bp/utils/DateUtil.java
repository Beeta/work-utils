package bp.utils;
/**
 * Created with Project: dateutil
 * Date: 2017/11/27　16:56
 * Aauthor: Casey
 * Description:
 */
import org.javatuples.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class DateUtil {
    //自定义pattren
    public static String currentDate(String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        return df.format(new Date());
    }

    // 自定义格式日期之间的转换
    public static String dateConvert(String day, String currentPattern, String pattern) {

        SimpleDateFormat currentDf = new SimpleDateFormat(currentPattern);
        Date date = null;
        try {
            date = currentDf.parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        SimpleDateFormat df = new SimpleDateFormat(pattern);
        return df.format(date);
    }

    // 获取string类型的当前时间
    public static String currentDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(new Date());
    }

    // 获取指定日期的毫秒数
    public static long getlongDate(String day, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        long longDate = -1;
        try {
            longDate = df.parse(day).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return longDate;
    }

    // 毫秒时间转格式化日期
    public static String convertMs(long time, String pattern) {
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(date);
    }

    public static String convertMs(long time) {
        return convertMs(time, "yyyy-MM-dd HH:mm:ss");
    }

    public static long getlongDate(String day) {
        return getlongDate(day, "yyyy-MM-dd");
    }

    // 获取当前时间是第几周
    public static int weekOfDay(String day) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = df.parse(day);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.setFirstDayOfWeek(2); // 设定星期一为一周之首,默认是周日
            return cal.get(Calendar.WEEK_OF_YEAR);
        } catch (ParseException e) {
            e.printStackTrace();
            return -1;
        }
    }

    // 获取当前时间是第几个月
    public static int monthOfDay(String day) {
        return monthOfDay(day, "yyyy-MM-dd");
    }


    public static int monthOfDay(String day, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        try {
            Date date = df.parse(day);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            return cal.get(Calendar.MONTH) + 1;
        } catch (ParseException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * 计算是周几
     *
     * @param day
     * @return
     */
    public static int dayOfWeek(String day) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = df.parse(day);

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int week = cal.get(Calendar.DAY_OF_WEEK);
            if (week == 1)
                return 7;
            else
                return week - 1;
        } catch (ParseException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * 时间相减: day - lastDays
     *
     * @param day
     * @param lastDays
     * @return
     */
    public static String dayBefore(String day, int lastDays) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = df.parse(day);
            return df.format(new Date(date.getTime() - (long) lastDays * 24 * 3600 * 1000));
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }


    // 时间相加 day + days
    public static String dayAfter(String day, int nextDays) {
        return dayBefore(day, 0 - nextDays);
    }

    // 时间相加减 小时 hour + hours
    public static String hourAfter(String day, int hour) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse(day);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (date == null)
            return "";
//        System.out.println("front:" + format.format(date)); //显示输入的日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, hour);// 24小时制
        date = cal.getTime();
//        System.out.println("after:" + format.format(date));  //显示更新后的日期
        cal = null;
        return format.format(date);
    }

    public static String hourBefore(String day, int hour) {
        return hourAfter(day, 0 - hour);

    }

    /**
     * 获取一个日期所在月的最后一天
     * @param day 日期
     * @return 日期
     */
    public static String getLastDayOfMonth(String day) {
        return getLastDayOfMonth(day, "yyyy-MM-dd");
    }

    public static String getLastDayOfMonth(String day, String pattern) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        try {
            calendar.setTime(format.parse(day));
            calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
            return format.format(calendar.getTime());
        } catch (Exception e) {
//            logger.error("date format error :" + day);
            return "-1";
        }
    }


    /**
     * Gets week period. 获取指定日期直到当前日期所在周的所有起始日期
     *
     * @param initDate the init date
     * @return the week period
     */
    public static List<Pair<String, String>> getWeekPeriod(String initDate) {
        List<Pair<String, String>> datePairList = new ArrayList<>();

        while (getlongDate(initDate) < getlongDate(currentDate())) {
            if (dayOfWeek(initDate) == 1) {
                datePairList.add(Pair.with(initDate, dayAfter(initDate, 6)));
            }
            initDate = dayAfter(initDate, 1);
        }

        return datePairList;

    }

    /**
     * 获取一段连续的时间段
     */
    public static List<String> getDateList(String startDate, String endDate) {
        List<String> list = new ArrayList<>();
        boolean matches0 = Pattern.matches("\\d{4}-\\d{2}-\\d{2}", startDate);
        boolean matches1 = Pattern.matches("\\d{4}-\\d{2}-\\d{2}", endDate);

        if (matches0 && matches1 && getlongDate(startDate) - getlongDate(endDate) <= 0) {
            String date = startDate;
            list.add(date);
            while (!date.equals(endDate)) {
                date = dayAfter(date, 1);
                list.add(date);
            }
        }
        return list;
    }
    
}
