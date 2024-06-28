package org.example.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.util.Date;

@Configuration
@EnableScheduling
public class BatchScheduler {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Scheduled(fixedRate = 60000) // Check every 60 seconds
    public void scheduleDatabaseJob() throws Exception {
        Job loadCsvToDatabaseJob = jobRegistry.getJob("loadCsvToDatabaseJob");

        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("timestamp", new Date()) // To ensure uniqueness
                .toJobParameters();
        jobLauncher.run(loadCsvToDatabaseJob, jobParameters);
    }

    @Scheduled(fixedRate = 60000) // Check every 60 seconds
    public void scheduleDownloadJob() throws Exception {
        Job loadCsvToDatabaseJob = jobRegistry.getJob("downloadCsvFileJob");

        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("timestamp", new Date()) // To ensure uniqueness
                .toJobParameters();
        jobLauncher.run(loadCsvToDatabaseJob, jobParameters);
    }
}