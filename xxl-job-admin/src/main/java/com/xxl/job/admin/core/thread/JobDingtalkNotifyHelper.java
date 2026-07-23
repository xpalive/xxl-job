package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 任务完成后通过钉钉发送通知的线程助手
 *
 * 参照 XXL-Job 现有 Thread 设计规范实现：
 *  - 单例
 *  - 独立守护线程
 *  - 阻塞队列 + 事件驱动
 *  - toStop 协作式终止
 *
 * 当前实现仅打印日志（成功/失败均打印），后续可扩展为真正的钉钉通知。
 *
 * 打印内容包括：
 *  - 执行器名称
 *  - 任务名称
 *  - 任务开始时间
 *  - 任务结束时间
 *  - 任务耗时
 *  - 执行状态与结果消息
 *
 * @author xiongping
 */
public class JobDingtalkNotifyHelper {

    private static final Logger logger = LoggerFactory.getLogger(JobDingtalkNotifyHelper.class);

    private static final JobDingtalkNotifyHelper instance = new JobDingtalkNotifyHelper();

    public static JobDingtalkNotifyHelper getInstance() {
        return instance;
    }

    /**
     * 任务完成通知事件队列（存 logId）
     * 容量 10000，队列满时不阻塞主流程，只打警告日志
     */
    private final LinkedBlockingQueue<Long> completeLogIdQueue = new LinkedBlockingQueue<>(10000);

    private Thread notifyThread;
    private volatile boolean toStop = false;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 推入完成事件（由 JobCompleteHelper.callback() 调用）
     * 非阻塞式，队列满则丢弃并打警告，绝不影响主流程
     */
    public void pushCompleteEvent(long logId) {
        boolean success = completeLogIdQueue.offer(logId);
        if (!success) {
            logger.warn(">>>>>>>>>>> xxl-job, JobDingtalkNotifyHelper queue is full, drop logId={}", logId);
        }
    }

    public void start() {
        notifyThread = new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info(">>>>>>>>>>> xxl-job, JobDingtalkNotifyHelper thread start.");

                while (!toStop) {
                    Long logId = null;
                    try {
                        // 阻塞取事件，3秒超时用于及时响应 toStop
                        logId = completeLogIdQueue.poll(3L, TimeUnit.SECONDS);
                        if (logId == null) {
                            continue;
                        }

                        doNotify(logId);

                    } catch (InterruptedException e) {
                        // 被 interrupt 唤醒（比如 toStop 时），下一轮循环退出
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobDingtalkNotifyHelper interrupted unexpectedly", e);
                        }
                    } catch (Throwable e) {
                        // 单条通知失败不影响后续处理
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobDingtalkNotifyHelper notify error, logId={}", logId, e);
                        }
                    }
                }

                logger.info(">>>>>>>>>>> xxl-job, JobDingtalkNotifyHelper thread stop.");
            }
        });
        notifyThread.setDaemon(true);
        notifyThread.setName("xxl-job, admin JobDingtalkNotifyHelper");
        notifyThread.start();
    }

    public void toStop() {
        toStop = true;

        if (notifyThread != null) {
            notifyThread.interrupt();
            try {
                notifyThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 执行通知逻辑（当前仅打印日志，后续可扩展为钉钉发送）
     */
    private void doNotify(long logId) {
        // 1. 查询任务日志
        XxlJobLog jobLog = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(logId);
        if (jobLog == null) {
            logger.warn(">>>>>>>>>>> xxl-job, JobDingtalkNotifyHelper log not found, logId={}", logId);
            return;
        }

        // 2. 查询任务信息（获取任务描述、handler）
        XxlJobInfo jobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobLog.getJobId());

        // 3. 查询执行器信息（获取 appname/title）
        XxlJobGroup jobGroup = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobLog.getJobGroup());

        // 4. 计算基础字段
        String executorName = buildExecutorName(jobGroup);
        String jobName = buildJobName(jobInfo, jobLog);
        Date triggerTime = jobLog.getTriggerTime();
        Date handleTime = jobLog.getHandleTime();
        String triggerTimeStr = triggerTime != null ? dateFormat.format(triggerTime) : "-";
        String handleTimeStr = handleTime != null ? dateFormat.format(handleTime) : "-";
        long cost = calcCostMillis(triggerTime, handleTime);
        String costStr = formatCost(cost);
        boolean success = jobLog.getHandleCode() == ReturnT.SUCCESS_CODE;
        String status = success ? "SUCCESS" : "FAIL";
        String handleMsg = truncate(jobLog.getHandleMsg(), 500);

        // 5. 打印通知日志（成功/失败均打印，后续可替换为钉钉调用）
        if (success) {
            logger.info(">>>>>>>>>>> xxl-job dingtalk notify [SUCCESS] " +
                            "executor=[{}], job=[{}], triggerTime=[{}], handleTime=[{}], cost=[{}], logId=[{}]",
                    executorName, jobName, triggerTimeStr, handleTimeStr, costStr, logId);
        } else {
            logger.warn(">>>>>>>>>>> xxl-job dingtalk notify [FAIL] " +
                            "executor=[{}], job=[{}], triggerTime=[{}], handleTime=[{}], cost=[{}], logId=[{}], msg=[{}]",
                    executorName, jobName, triggerTimeStr, handleTimeStr, costStr, logId, handleMsg);
        }

        // TODO: 后续在此处调用钉钉 webhook，参数已在上面组装好
        // sendDingtalk(executorName, jobName, triggerTimeStr, handleTimeStr, costStr, status, handleMsg);
    }

    /**
     * 执行器名称：优先取 title，其次 appname，最后用 groupId 兜底
     */
    private String buildExecutorName(XxlJobGroup jobGroup) {
        if (jobGroup == null) {
            return "unknown";
        }
        if (jobGroup.getTitle() != null && !jobGroup.getTitle().trim().isEmpty()) {
            return jobGroup.getTitle() + "(" + jobGroup.getAppname() + ")";
        }
        if (jobGroup.getAppname() != null && !jobGroup.getAppname().trim().isEmpty()) {
            return jobGroup.getAppname();
        }
        return "groupId=" + jobGroup.getId();
    }

    /**
     * 任务名称：优先取 jobDesc，其次 executorHandler，最后用 jobId 兜底
     */
    private String buildJobName(XxlJobInfo jobInfo, XxlJobLog jobLog) {
        if (jobInfo != null && jobInfo.getJobDesc() != null && !jobInfo.getJobDesc().trim().isEmpty()) {
            return jobInfo.getJobDesc();
        }
        if (jobLog.getExecutorHandler() != null && !jobLog.getExecutorHandler().trim().isEmpty()) {
            return jobLog.getExecutorHandler();
        }
        return "jobId=" + jobLog.getJobId();
    }

    /**
     * 计算任务耗时（毫秒），任一时间为空则返回 -1
     */
    private long calcCostMillis(Date triggerTime, Date handleTime) {
        if (triggerTime == null || handleTime == null) {
            return -1;
        }
        return handleTime.getTime() - triggerTime.getTime();
    }

    /**
     * 格式化耗时输出
     */
    private String formatCost(long cost) {
        if (cost < 0) {
            return "-";
        }
        if (cost < 1000) {
            return cost + "ms";
        }
        long seconds = cost / 1000;
        long remainMs = cost % 1000;
        if (seconds < 60) {
            return seconds + "s" + remainMs + "ms";
        }
        long minutes = seconds / 60;
        long remainSec = seconds % 60;
        return minutes + "m" + remainSec + "s";
    }

    /**
     * 截断超长消息（钉钉消息有长度限制，日志里也不用打太长）
     */
    private String truncate(String s, int maxLen) {
        if (s == null) {
            return "";
        }
        if (s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen) + "...";
    }

}
