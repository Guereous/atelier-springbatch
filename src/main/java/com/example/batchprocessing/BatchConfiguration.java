package com.example.batchprocessing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
//    private DataSource dataSource; //

    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited().delimiter(";")
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
//        this.dataSource = dataSource; //
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) throws IOException {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .next()
//                .next(step2(flatWriter())) //
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Person> writer) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(3)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }

    //step2----------------------------------------------------------------------

//    @Bean
//    Step step2(FlatFileItemWriter<Person> flatWriter) {
//        return stepBuilderFactory.get("step2")
//                .<Person, Person>chunk(3)
//                .reader(jdbcReader(dataSource))
////				.processor(processor())
//                .writer(flatWriter)
//                .build();
//    }
//
//    @Bean
//    public JdbcCursorItemReader<Person> jdbcReader(DataSource dataSource) {
//
//        JdbcCursorItemReader<Person> databaseReader = new JdbcCursorItemReader<>();
//
//        databaseReader.setDataSource(dataSource);
//        databaseReader.setSql("SELECT first_name, last_name from people");
//        databaseReader.setRowMapper(new BeanPropertyRowMapper<>
//                (Person.class));
//
//        return databaseReader;
//    }
//
//    @Bean
//    public FlatFileItemWriter<Person> flatWriter() throws IOException {
//
//        FlatFileItemWriter<Person> flatWriter = new FlatFileItemWriter<>();
//
//        File fichierSortie = new File("src/main/resources/flatDataOut.csv");
//        fichierSortie.createNewFile();
//        ClassPathResource fileResource = new ClassPathResource("flatDataOut.csv");
//
//        //Set output file location
//        flatWriter.setResource(fileResource);
//
//        //All job repetitions should "append" to same output file
//        flatWriter.setAppendAllowed(true);
//
//        //Name field values sequence based on object properties
//        flatWriter.setLineAggregator(new DelimitedLineAggregator<Person>() {
//            {
//                setDelimiter("BADABOUM");
//                setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {
//                    {
//                        setNames(new String[]{"firstName", "lastName"});
//                    }
//                });
//            }
//        });
//        return flatWriter;
//    }

}
