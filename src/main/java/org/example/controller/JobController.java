package org.example.controller;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
@RequestMapping("/jobs")
public class JobController {
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;


    @GetMapping("/start")
    public String startJob(@RequestParam String jobName) {
        try {
            Job job = jobRegistry.getJob(jobName);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addDate("timestamp", new Date())
                    .toJobParameters();
            jobLauncher.run(job, jobParameters);
            return jobName + " started successfully.";
        } catch (Exception e) {
            return "Failed to start " + jobName + ": " + e.getMessage();
        }
    }
}
