package bdtc.lab1;

/**
 * Типы счетчиков для подсчета статистики по битым логам.
 * WRONG_NUMBER_OF_FIELDS - счетчик, отвечающий за число логов, имеющих  число полей, отличного от 5.
 * SEVERITY_ERROR - счетчик, отвечающий за число логов, имеющих число полей равное 5, но со значением severity, лежащем
 * не в интервале от 0 до 7.
 */
public enum CounterType {
    WRONG_NUMBER_OF_FIELDS,
    SEVERITY_ERROR
}
