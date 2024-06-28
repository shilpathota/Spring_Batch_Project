package org.example.config;

import org.example.entity.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;


import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.*;

@Configuration
public class SpringBatchConfig {
    private static final Logger logger = LoggerFactory.getLogger(SpringBatchConfig.class);

    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private DataSource dataSource;
    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor processor = new JobRegistryBeanPostProcessor();
        processor.setJobRegistry(jobRegistry);
        return processor;
    }
    @Bean
    public Job downloadCsvFileJob(Step downloadCsvFileStep) {
        return new JobBuilder("downloadCsvFileJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(downloadCsvFileStep)
                .build();
    }

    @Bean
    public Step downloadCsvFileStep(Tasklet downloadCsvFileTasklet) {
        return new StepBuilder("downloadCsvFileStep", jobRepository)
                .tasklet(downloadCsvFileTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet downloadCsvFileTasklet(
            @Value("Customer2.csv") String sourceFileUrl,
            @Value("Customer.csv") String targetFilePath
    ) throws MalformedURLException {
        return new DownloadCsvFileTasklet(new ClassPathResource(sourceFileUrl), new ClassPathResource(targetFilePath));
    }

    @Bean
    public Job loadCsvToDatabaseJob(Step loadCsvToDatabaseStep) {
        return new JobBuilder("loadCsvToDatabaseJob", jobRepository)
                .start(loadCsvToDatabaseStep)
                .build();
    }

    @Bean
    public Step loadCsvToDatabaseStep(
            ItemReader<Customer> reader,
            ItemProcessor<Customer, Customer> processor,
            ItemWriter<Customer> writer
    ) {
        return new StepBuilder("loadCsvToDatabaseStep", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

@Bean
public FlatFileItemReader<Customer> reader() {
    return new FlatFileItemReaderBuilder<Customer>()
            .name("customerItemReader")
            .resource(new ClassPathResource("Customer.csv"))
            .linesToSkip(1)//skip the header
            .lineMapper(lineMapper())
            .delimited()
            .names(new String[]{"id", "firstName", "lastName", "gender", "email", "country"})
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Customer>() {{
                setTargetType(Customer.class);
            }})
            .build();
}

    @Bean
    public LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer() {
            @Override
            public FieldSet tokenize(String line) {
                if (line.startsWith("\uFEFF")) {
                    line = line.substring(1);
                }
                return super.tokenize(line);
            }
        };
        lineTokenizer.setNames("ID", "FIRSTNAME", "LASTNAME", "GENDER", "EMAIL", "COUNTRY");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }
    @Bean
    public ItemProcessor<Customer, Customer> processor() {
        return new CustomerProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Customer> writer() {
        JdbcBatchItemWriter<Customer> writer = new JdbcBatchItemWriterBuilder<Customer>()
                .sql("INSERT INTO customer_info (customer_id, first_name, last_name, gender, email_id, country) " +
                        "VALUES (:id, :firstName, :lastName, :gender, :email, :country) " +
                        "ON CONFLICT (customer_id) DO NOTHING")
                .dataSource(dataSource)
                .beanMapped()
                .build();
        writer.setAssertUpdates(false);
        return writer;
    }

    private static class DownloadCsvFileTasklet implements Tasklet {
        private final Resource url;
        private final Resource path;

        public DownloadCsvFileTasklet(final Resource url, final Resource path) {
            this.url = url;
            this.path = path;
        }

        @Override
        public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) {
            try {
                downloadCsvFile(url, path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return RepeatStatus.FINISHED;
        }

        private static void downloadCsvFile(final Resource url, final Resource path) throws IOException {
                Files.write(Paths.get(url.getURI()), path.getContentAsByteArray(), StandardOpenOption.APPEND);
                logger.info("File '{}' has been downloaded from '{}'", path, url);
        }
    }

}