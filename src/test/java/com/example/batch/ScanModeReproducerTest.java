package com.example.batch;

import jakarta.persistence.Entity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Version;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = ScanModeReproducerApplication.class)
@Import(ScanModeReproducerTest.BatchConfiguration.class)
class ScanModeReproducerTest {

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job taskletJob;

    @Autowired
    private Job chunkOrientedJob;

    @Autowired
    private Recording recording;

    @BeforeEach
    void resetRecording() {
        recording.reset();
    }

    @Test
    void taskletStepPathReportsTheOriginalBusinessException() throws Exception {
        JobExecution execution = run(taskletJob);

        assertThat(recording.skipExceptions).singleElement().isInstanceOf(BusinessValidationException.class);
        assertThat(execution.getExitStatus().getExitCode()).isEqualTo("COMPLETED");
        // Three inputs plus a scan-mode re-processing of the failed input.
        assertThat(recording.processorInvocations.get()).isEqualTo(4);
    }

    @Test
    void chunkOrientedStepPathReportsTheOriginalBusinessException() throws Exception {
        JobExecution execution = run(chunkOrientedJob);

        // The new path scans its cached output instead of re-invoking the processor.
        assertThat(recording.processorInvocations.get()).isEqualTo(3);
        // Desired behaviour: this assertion fails on Spring Batch 6.0.4. The actual exception is a
        // PropertyValueException about the stale entity's uninitialized version value.
        assertThat(recording.skipExceptions).singleElement().isInstanceOf(BusinessValidationException.class);
        assertThat(execution.getExitStatus().getExitCode()).isEqualTo("COMPLETED");
    }

    private JobExecution run(Job job) throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLong("run.id", System.nanoTime())
                .toJobParameters();
        return jobOperator.start(job, parameters);
    }

    @TestConfiguration
    @EnableBatchProcessing
    static class BatchConfiguration {

        @Bean
        Recording recording() {
            return new Recording();
        }

        @Bean
        Job taskletJob(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                       EntityManager entityManager, Recording recording) {
            return new JobBuilder("taskletJob", jobRepository)
                    .start(new StepBuilder("taskletStep", jobRepository)
                            .<String, Product>chunk(1, transactionManager)
                            .reader(reader())
                            .processor(processor(recording))
                            .writer(writer(entityManager))
                            .faultTolerant()
                            .skip(Exception.class)
                            .skipLimit(10)
                            .listener(skipListener(recording))
                            .build())
                    .build();
        }

        @Bean
        Job chunkOrientedJob(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             EntityManager entityManager, Recording recording) {
            return new JobBuilder("chunkOrientedJob", jobRepository)
                    .start(new StepBuilder("chunkOrientedStep", jobRepository)
                            .<String, Product>chunk(1)
                            .transactionManager(transactionManager)
                            .reader(reader())
                            .processor(processor(recording))
                            .writer(writer(entityManager))
                            .faultTolerant()
                            .skip(Exception.class)
                            .skipLimit(10)
                            .skipListener(skipListener(recording))
                            .build())
                    .build();
        }

        private ListItemReader<String> reader() {
            return new ListItemReader<>(List.of("valid-first", "invalid", "valid-last"));
        }

        private ItemProcessor<String, Product> processor(Recording recording) {
            return name -> {
                recording.processorInvocations.incrementAndGet();
                return new Product(name, name.equals("invalid"));
            };
        }

        private ItemWriter<Product> writer(EntityManager entityManager) {
            return items -> {
                for (Product product : items) {
                    entityManager.persist(product);
                }
                entityManager.flush();
            };
        }

        private SkipListener<String, Product> skipListener(Recording recording) {
            return new SkipListener<>() {
                @Override
                public void onSkipInWrite(Product item, Throwable throwable) {
                    recording.skipExceptions.add(throwable);
                }
            };
        }
    }

    static class Recording {
        private final AtomicInteger processorInvocations = new AtomicInteger();
        private final List<Throwable> skipExceptions = new ArrayList<>();

        void reset() {
            processorInvocations.set(0);
            skipExceptions.clear();
        }
    }

    @Entity(name = "Product")
    static class Product {
        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE)
        private Long id;

        @Version
        private Long version;

        private String name;

        private boolean invalid;

        Product() {
        }

        Product(String name, boolean invalid) {
            this.name = name;
            this.invalid = invalid;
        }

        @PrePersist
        void validate() {
            if (invalid) {
                throw new BusinessValidationException("product is invalid: " + name);
            }
        }
    }

    static class BusinessValidationException extends RuntimeException {
        BusinessValidationException(String message) {
            super(message);
        }
    }
}
