<?php
/**
 * Some code comes from Medoo <http://medoo.in/>
 * Copyright 2018, Angel Lai
 */

namespace Hail\Database\Sql;

use PDO;

/**
 * SQL builder like Medoo
 *
 * @package Hail\Database
 * @author  FENG Hao <flyinghail@msn.com>
 */
class Builder
{
    /**
     * @var string
     */
    protected $type;

    /**
     * @var string
     */
    protected $prefix = '';

    /**
     * @var string
     */
    protected $quote = '"';

    /**
     * @var int
     */
    protected $guid = 0;

    /**
     * @var string
     */
    protected $sql;

    /**
     * @var array
     */
    protected $map;

    public function __construct(string $type, string $prefix = null)
    {
        if ($type === 'mariadb' || $type === 'mysql') {
            $type = 'mysql';
            $this->quote = '`';
        }

        $this->type = $type;

        if ($prefix) {
            $this->prefix = $prefix;
        }
    }

    public function quote(string $string): string
    {
        return $this->quote . $string . $this->quote;
    }

    public function raw(string $string, array $map = []): Raw
    {
        return new Raw($string, $map);
    }

    public function buildRaw($raw): ?string
    {
        if (!$raw instanceof Raw) {
            return null;
        }

        $query = \preg_replace_callback(
            '/(([`\']).*?)?((FROM|TABLE|INTO|UPDATE|JOIN)\s*)?\<((\w+)(\.\w+)?)\>(.*?\2)?/i',
            [$this, 'buildRawCallback'],
            $raw->value
        );

        $rawMap = $raw->map;

        if (!empty($rawMap)) {
            foreach ($rawMap as $key => $value) {
                $this->map[$key] = $this->typeMap($value);
            }
        }

        return $query;
    }

    protected function buildRawCallback(array $matches): string
    {
        if (!empty($matches[2]) && isset($matches[8])) {
            return $matches[0];
        }

        if (!empty($matches[4])) {
            return $matches[1] . $matches[4] . ' ' . $this->tableQuote($matches[5]);
        }

        return $matches[1] . $this->columnQuote($matches[5]);
    }

    protected function typeMap($value): array
    {
        static $map = [
            'NULL' => PDO::PARAM_NULL,
            'integer' => PDO::PARAM_INT,
            'boolean' => PDO::PARAM_BOOL,
            'resource' => PDO::PARAM_LOB,
            'object' => PDO::PARAM_STR,
            'double' => PDO::PARAM_STR,
            'string' => PDO::PARAM_STR,
            'array' => PDO::PARAM_STR,
        ];

        $type = \gettype($value);

        if ($type === 'boolean') {
            $value = ($value ? '1' : '0');
        } elseif ($type === 'NULL') {
            $value = null;
        } elseif ($type === 'array') {
            $value = \json_encode($value,
                \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES | \JSON_PRESERVE_ZERO_FRACTION
            );
        }

        return [$value, $map[$type] ?? PDO::PARAM_STR];
    }

    /**
     * @param string $prefix
     *
     * @return $this
     */
    public function setPrefix(string $prefix): self
    {
        $this->prefix = $prefix;

        return $this;
    }

    /**
     * @param string $quote
     *
     * @return $this
     */
    public function setQuote(string $quote): self
    {
        $this->quote = $quote;

        return $this;
    }

    /**
     * @return string
     */
    public function getSql(): string
    {
        return $this->sql ?? '';
    }

    /**
     * @return array
     */
    public function getMap(): array
    {
        return $this->map ?? [];
    }

    protected function tableQuote($table)
    {
        if (!\preg_match('/^\w+(\.\w+)?$/', $table)) {
            throw new \InvalidArgumentException('Incorrect table name "' . $table . '"');
        }

        if (\strpos($table, '.') !== false) { // database.table
            return $this->quote(
                \str_replace(
                    '.',
                    $this->quote('.') . $this->prefix,
                    $table
                )
            );
        }

        return $this->quote($this->prefix . $table);
    }

    protected function mapKey(): string
    {
        $index = (string)$this->guid;
        ++$this->guid;

        return ":HA_{$index}_IL";
    }

    protected function columnQuote(string $string): string
    {
        if ($string === '*') {
            return '*';
        }

        if (!\preg_match('/^\w+(\.\w+)?$/', $string)) {
            throw new \InvalidArgumentException('Incorrect column name "' . $string . '"');
        }

        if (($p = \strpos($string, '.')) !== false) { // table.column
            if ($string[$p + 1] === '*') {// table.*
                return $this->quote(
                        $this->prefix . \substr($string, 0, $p)
                    ) . '.*';
            }

            return $this->quote(
                $this->prefix . \str_replace('.', $this->quote('.'), $string)
            );
        }

        return $this->quote($string);
    }

    protected function columnPush($columns, bool $isJoin = false): string
    {
        if ($columns === '*') {
            return $columns;
        }

        if (\is_string($columns)) {
            $columns = [$columns];
        }

        $stack = [];
        foreach ($columns as $key => $value) {
            if (\is_array($value)) {
                $stack[] = $this->columnPush($value, $isJoin);
                continue;
            }

            if (!\is_int($key)) {
                if ($raw = $this->buildRaw($value)) {
                    $stack[] = $raw . ' AS ' . $this->columnQuote($key);
                } elseif (\is_string($value)) {
                    $stack[] = $this->columnQuote($key) . ' AS ' . $this->columnQuote($value);
                }
                continue;
            }

            if (\is_string($value)) {
                if ($isJoin && \strpos($value, '*') !== false) {
                    throw new \InvalidArgumentException('Cannot use table.* to select all columns while joining table');
                }

                \preg_match('/(?<column>[a-zA-Z0-9_\.]+)(?:\s*\((?<alias>\w+)\))?/i', $value, $match);

                if (!empty($match['alias'])) {
                    $stack[] = $this->columnQuote($match['column']) . ' AS ' . $this->columnQuote($match['alias']);
                } else {
                    $stack[] = $this->columnQuote($match['column']);
                }
            }
        }

        return \implode(',', $stack);
    }


    protected function arrayQuote(array $array): string
    {
        $key = $this->mapKey();

        $temp = [];
        foreach ($array as $i => $value) {
            $temp[] = $key . 'O' . $i;
            $this->map[$key . 'O' . $i] = $this->typeMap($value);
        }

        return \implode(',', $temp);
    }

    protected function innerConjunct(array $data, string $conjunctor, string $outerConjunctor): string
    {
        $stack = [];
        foreach ($data as $value) {
            $stack[] = '(' . $this->dataImplode($value, $conjunctor) . ')';
        }

        return \implode($outerConjunctor . ' ', $stack);
    }

    protected function dataImplode(array $data, string $conjunctor)
    {
        $stack = [];

        foreach ($data as $key => $value) {
            $type = \gettype($value);

            if (
                $type === 'array' &&
                \preg_match("/^(AND|OR)(\s+#.*)?$/", $key, $relation_match)
            ) {
                $relationship = $relation_match[1];

                $stack[] = $value !== \array_keys(\array_keys($value)) ?
                    '(' . $this->dataImplode($value, ' ' . $relationship) . ')' :
                    '(' . $this->innerConjunct($value, ' ' . $relationship, $conjunctor) . ')';

                continue;
            }

            $mapKey = $this->mapKey();

            if (
                \is_int($key) &&
                \preg_match('/([a-zA-Z0-9_\.]+)\[(?<operator>\>\=?|\<\=?|\!?\=)\]([a-zA-Z0-9_\.]+)/i', $value, $match)
            ) {
                $stack[] = $this->columnQuote($match[1]) . ' ' . $match['operator'] . ' ' . $this->columnQuote($match[3]);
            } else {
                \preg_match('/([a-zA-Z0-9_\.]+)(\[(?<operator>\>\=?|\<\=?|\!|\<\>|\>\<|\!?~|REGEXP)\])?/i', $key,
                    $match);
                $column = $this->columnQuote($match[1]);

                if (isset($match['operator'])) {
                    $operator = $match['operator'];

                    if (\in_array($operator, ['>', '>=', '<', '<='], true)) {
                        $condition = $column . ' ' . $operator . ' ';

                        if (\is_numeric($value)) {
                            $condition .= $mapKey;
                            $this->map[$mapKey] = [$value, \is_float($value) ? PDO::PARAM_STR : PDO::PARAM_INT];
                        } elseif ($raw = $this->buildRaw($value)) {
                            $condition .= $raw;
                        } else {
                            $condition .= $mapKey;
                            $this->map[$mapKey] = [$value, PDO::PARAM_STR];
                        }

                        $stack[] = $condition;
                    } elseif ($operator === '!') {
                        switch ($type) {
                            case 'NULL':
                                $stack[] = $column . ' IS NOT NULL';
                                break;

                            case 'array':
                                $placeholders = [];

                                foreach ($value as $index => $item) {
                                    $selectKey = $mapKey . $index . '_i';
                                    $placeholders[] = $selectKey;
                                    $this->map[$selectKey] = $this->typeMap($item);
                                }

                                $stack[] = $column . ' NOT IN (' . \implode(', ', $placeholders) . ')';
                                break;

                            case 'object':
                                if ($raw = $this->buildRaw($value)) {
                                    if (\strpos($raw, 'SELECT') === 0) {
                                        $stack[] = $column . ' NOT IN (' . $raw . ')';
                                    } else {
                                        $stack[] = $column . ' != ' . $raw;
                                    }
                                }
                                break;

                            case 'integer':
                            case 'double':
                            case 'boolean':
                            case 'string':
                                $stack[] = $column . ' != ' . $mapKey;
                                $this->map[$mapKey] = $this->typeMap($value, $type);
                                break;
                        }
                    } elseif ($operator === '~' || $operator === '!~') {
                        if ($type !== 'array') {
                            $value = [$value];
                        }

                        $connector = ' OR ';
                        $data = \array_values($value);

                        if (\is_array($data[0])) {
                            if (isset($value[SQL:: AND]) || isset($value[SQL:: OR])) {
                                $connector = ' ' . \array_keys($value)[0] . ' ';
                                $value = $data[0];
                            }
                        }

                        $like = [];

                        foreach ($value as $index => $item) {
                            $item = (string)$item;

                            if (!\preg_match('/(\[.+]|[*?!%#^-_]|%.+|.+%)/', $item)) {
                                $item = '%' . $item . '%';
                            }

                            $like[] = $column . ($operator === '!~' ? ' NOT' : '') . ' LIKE ' . $mapKey . 'L' . $index;
                            $this->map[$mapKey . 'L' . $index] = [$item, PDO::PARAM_STR];
                        }

                        $stack[] = '(' . \implode($connector, $like) . ')';
                    } elseif ($operator === '<>' || $operator === '><') {
                        if ($type === 'array') {
                            if ($operator === '><') {
                                $column .= ' NOT';
                            }

                            $stack[] = '(' . $column . ' BETWEEN ' . $mapKey . 'a AND ' . $mapKey . 'b)';

                            $dataType = (\is_numeric($value[0]) && \is_numeric($value[1])) ? PDO::PARAM_INT : PDO::PARAM_STR;

                            $this->map[$mapKey . 'a'] = [$value[0], $dataType];
                            $this->map[$mapKey . 'b'] = [$value[1], $dataType];
                        }
                    } elseif ($operator === 'REGEXP') {
                        $stack[] = $column . ' REGEXP ' . $mapKey;
                        $this->map[$mapKey] = [$value, PDO::PARAM_STR];
                    }
                } else {
                    switch ($type) {
                        case 'NULL':
                            $stack[] = $column . ' IS NULL';
                            break;

                        case 'array':
                            $placeholders = [];

                            foreach ($value as $index => $item) {
                                $selectKey = $mapKey . $index . '_i';
                                $placeholders[] = $selectKey;
                                $this->map[$selectKey] = $this->typeMap($item);
                            }

                            $stack[] = $column . ' IN (' . \implode(', ', $placeholders) . ')';
                            break;

                        case 'object':
                            if ($raw = $this->buildRaw($value)) {
                                if (\strpos($raw, 'SELECT') === 0) {
                                    $stack[] = $column . ' IN (' . $raw . ')';
                                } else {
                                    $stack[] = $column . ' != ' . $raw;
                                }
                            }
                            break;

                        case 'integer':
                        case 'double':
                        case 'boolean':
                        case 'string':
                            $stack[] = $column . ' = ' . $mapKey;
                            $this->map[$mapKey] = $this->typeMap($value, $type);
                            break;
                    }
                }
            }
        }

        return \implode($conjunctor . ' ', $stack);
    }

    /**
     * @param array|Raw $struct
     *
     * @return string
     */
    protected function whereClause($struct): string
    {
        if (\is_array($struct)) {
            if (!isset($struct[SQL::WHERE])) {
                static $keys = [SQL::GROUP, SQL::ORDER, SQL::LIMIT, SQL::HAVING, SQL::MATCH];

                $temp = [];
                foreach ($keys as $key) {
                    if (isset($struct[$key])) {
                        $temp[$key] = $struct[$key];
                        unset($struct[$key]);
                    }
                }
                $temp[SQL::WHERE] = $struct;

                $struct = $temp;
            }

            return $this->suffixClause($struct);
        }

        if ($raw = $this->buildRaw($struct)) {
            return $raw;
        }

        throw new \InvalidArgumentException('Where clause must be array or ' . Raw::class);
    }

    protected function suffixClause(array $struct): string
    {
        if (empty($struct)) {
            return '';
        }

        $clause = '';
        if (isset($struct[SQL::WHERE])) {
            $where = $struct[SQL::WHERE];
            if (\is_array($where)) {
                if (!empty($where)) {
                    $clause = ' WHERE ' . $this->dataImplode($where, ' AND');
                }
            } elseif ($raw = $this->buildRaw($where)) {
                $clause .= ' ' . $raw;
            }
        }

        if (isset($struct[SQL::MATCH]) && $this->type === 'mysql') {
            $MATCH = $struct[SQL::MATCH];

            $matchClause = '';
            if (\is_array($MATCH)) {
                if (isset($MATCH['columns'], $MATCH['keyword'])) {
                    $mode = '';

                    static $mode_array = [
                        'natural' => 'IN NATURAL LANGUAGE MODE',
                        'natural+query' => 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION',
                        'boolean' => 'IN BOOLEAN MODE',
                        'query' => 'WITH QUERY EXPANSION',
                    ];

                    if (isset($MATCH['mode'], $mode_array[$MATCH['mode']])) {
                        $mode = ' ' . $mode_array[$MATCH['mode']];
                    }

                    $columns = \implode(', ', \array_map([$this, 'columnQuote'], $MATCH['columns']));
                    $mapKey = $this->mapKey();
                    $this->map[$mapKey] = [$MATCH['keyword'], PDO::PARAM_STR];

                    $matchClause = ' MATCH (' . $columns . ') AGAINST (' . $mapKey . $mode . ')';
                }
            } elseif ($raw = $this->buildRaw($MATCH)) {
                $matchClause = $raw;
            }

            if ($matchClause !== '') {
                $clause .= ($clause !== '' ? ' AND ' : ' WHERE ') . $matchClause;
            }
        }

        if (isset($struct[SQL::GROUP])) {
            $GROUP = $struct[SQL::GROUP];

            if (\is_array($GROUP)) {
                $clause .= ' GROUP BY ' . \implode(',', \array_map([$this, 'columnQuote'], $GROUP));
            } elseif ($raw = $this->buildRaw($GROUP)) {
                $clause .= ' GROUP BY ' . $raw;
            } else {
                $clause .= ' GROUP BY ' . $this->columnQuote($GROUP);
            }

            if (isset($struct[SQL::HAVING])) {
                $HAVING = $struct[SQL::HAVING];
                if ($raw = $this->buildRaw($HAVING)) {
                    $clause .= ' HAVING ' . $raw;
                } else {
                    $clause .= ' HAVING ' . $this->dataImplode($HAVING, ' AND');
                }
            }
        }

        if (isset($struct[SQL::ORDER])) {
            $ORDER = $struct[SQL::ORDER];

            if (\is_array($ORDER)) {
                $stack = [];

                foreach ($ORDER as $column => $value) {
                    if (\is_array($value)) {
                        $stack[] = 'FIELD(' . $this->columnQuote($column) . ', ' . $this->arrayQuote($value) . ')';
                    } elseif ($value === 'ASC' || $value === 'DESC') {
                        $stack[] = $this->columnQuote($column) . ' ' . $value;
                    } elseif (\is_int($column)) {
                        $stack[] = $this->columnQuote($value);
                    }
                }

                $clause .= ' ORDER BY ' . \implode(',', $stack);
            } elseif ($raw = $this->buildRaw($ORDER)) {
                $clause .= ' ORDER BY ' . $raw;
            } else {
                $clause .= ' ORDER BY ' . $this->columnQuote($ORDER);
            }
        }

        if (isset($struct[SQL::LIMIT])) {
            $LIMIT = $struct[SQL::LIMIT];
            if (\is_numeric($LIMIT)) {
                $clause .= ' LIMIT ' . $LIMIT;
            } elseif (
                \is_array($LIMIT) &&
                \is_numeric($LIMIT[0]) &&
                \is_numeric($LIMIT[1])
            ) {
                $clause .= ' LIMIT ' . $LIMIT[1] . ' OFFSET ' . $LIMIT[0];
            }
        }

        return $clause;
    }

    protected function getTable(array $struct, string $key = null): string
    {
        if ($key && isset($struct[$key])) {
            return $struct[$key];
        }

        if (!isset($struct[SQL::TABLE])) {
            throw new \InvalidArgumentException('SQL array must contains table.');
        }

        return $struct[SQL::TABLE];
    }

    /**
     * @param array|string $struct
     *
     * @return array
     */
    private function selectFormat($struct): array
    {
        if (\is_string($struct)) {
            return [SQL::TABLE => $struct];
        }

        if (!isset($struct[SQL::FROM])) {
            if (!isset($struct[SQL::TABLE])) {
                throw new \InvalidArgumentException('SQL must contains table.');
            }

            $struct[SQL::FROM] = $struct[SQL::TABLE];
            unset($struct[SQL::TABLE]);
        }

        if (!isset($struct[SQL::SELECT])) {
            if (isset($struct[SQL::COLUMNS])) {
                $struct[SQL::SELECT] = $struct[SQL::COLUMNS];
                unset($struct[SQL::COLUMNS]);
            } else {
                $struct[SQL::SELECT] = '*';
            }
        }

        return $struct;
    }

    public function create(array $struct): string
    {
        $table = $struct[SQL::CREATE] ?? $struct[SQL::TABLE] ?? null;
        if ($table === null) {
            throw new \InvalidArgumentException('SQL must be contains table');
        }

        $columns = $struct[SQL::COLUMNS] ?? null;
        if ($columns === null) {
            throw new \InvalidArgumentException('SQL must be contains columns');
        }

        $quote = $this->quote;

        $stack = [];
        foreach ($columns as $name => $definition) {
            if (\is_int($name)) {
                $stack[] = preg_replace('/\<(\w+)>/', $quote . '$1' . $quote, $definition);
            } elseif (\is_array($definition)) {
                $stack[] = $quote . $name . $quote . ' ' . implode(' ', $definition);
            } elseif (\is_string($definition)) {
                $stack[] = $quote . $name . $quote . ' ' . $definition;
            }
        }

        $options = $struct[SQL::OPTIONS] ?? null;

        $tableOption = '';
        if (\is_array($options)) {
            $optionStack = [];
            foreach ($options as $key => $value) {
                if (\is_string($value) || \is_int($value)) {
                    $optionStack[] = $key . ' = ' . $value;
                }
            }
            $tableOption = ' ' . \implode(', ', $optionStack);
        } elseif (\is_string($options)) {
            $tableOption = ' ' . $options;
        }

        $table = $this->tableQuote($table);

        return "CREATE TABLE IF NOT EXISTS $table (" . \implode(', ', $stack) . ")$tableOption";
    }

    public function drop(string $table): string
    {
        return 'DROP TABLE IF EXISTS ' . $this->tableQuote($table);
    }

    /**
     * @param string|array $struct
     *
     * @return array
     */
    public function select($struct): array
    {
        $struct = $this->selectFormat($struct);

        $this->map = [];

        $table = $struct[SQL::FROM];
        \preg_match('/(?<table>\w+)\s*\((?<alias>\w+)\)/i', $table, $tableMatch);

        if (isset($tableMatch['table'], $tableMatch['alias'])) {
            $table = $this->tableQuote($tableMatch['table']);
            $tableQuery = $table . ' AS ' . $this->tableQuote($tableMatch['alias']);
        } else {
            $table = $this->tableQuote($table);
            $tableQuery = $table;
        }

        $join = $struct[SQL::JOIN] ?? null;
        $isJoin = false;
        $joinKey = \is_array($join) ? \array_keys($join) : null;

        if (!empty($joinKey[0]) && $joinKey[0][0] === '[') {
            $isJoin = true;
            $tableQuery .= ' ' . $this->buildJoin($table, $join);
        }

        $columns = $struct[SQL::SELECT];
        if (isset($struct[SQL::FUN])) {
            $fn = $struct[SQL::FUN];
            if ($fn === 1 || $fn === '1') {
                $column = '1';
            } elseif ($raw = $this->buildRaw($fn)) {
                $column = $raw;
            } else {
                $column = $fn . '(' . $this->columnPush($columns) . ')';
            }
        } else {
            $column = $this->columnPush($columns, $isJoin);
        }

        $sql = 'SELECT ' . $column . ' FROM ' . $tableQuery . $this->suffixClause($struct);

        $this->sql = $sql;

        return [$sql, $this->map];
    }

    protected function buildJoin(string $table, array $join): string
    {
        $tableJoin = [];

        static $joinArray = [
            '>' => 'LEFT',
            '<' => 'RIGHT',
            '<>' => 'FULL',
            '><' => 'INNER',
        ];

        $quote = $this->quote;

        foreach ($join as $sub => $relation) {
            \preg_match('/(\[(?<join>\<\>?|\>\<?)\])?(?<table>\w+)\s?(\((?<alias>\w+)\))?/',
                $sub, $match);

            if ($match['join'] !== '' && $match['table'] !== '') {
                if (\is_string($relation)) {
                    $relation = 'USING (' . $quote . $relation . $quote . ')';
                }

                if (\is_array($relation)) {
                    // For ['column1', 'column2']
                    if (isset($relation[0])) {
                        $relation = 'USING (' . $quote . \implode($quote . ', ' . $quote, $relation) . $quote . ')';
                    } else {
                        $joins = [];

                        foreach ($relation as $key => $value) {
                            $joins[] = (
                                \strpos($key, '.') > 0 ?
                                    // For ['tableB.column' => 'column']
                                    $this->columnQuote($key) :

                                    // For ['column1' => 'column2']
                                    $table . '.' . $quote . $key . $quote
                                ) .
                                ' = ' .
                                $this->tableQuote($match['alias'] ?? $match['table']) . '.' . $quote . $value . $quote;
                        }

                        $relation = 'ON ' . \implode(' AND ', $joins);
                    }
                }

                $tableName = $this->tableQuote($match['table']) . ' ';

                if (isset($match['alias'])) {
                    $tableName .= 'AS ' . $this->tableQuote($match['alias']) . ' ';
                }

                $tableJoin[] = $joinArray[$match['join']] . ' JOIN ' . $tableName . $relation;
            }
        }

        return \implode(' ', $tableJoin);
    }

    /**
     * @param string|array $table
     * @param string|array $datas
     * @param string|array $INSERT
     *
     * @return array
     */
    public function insert($table, $datas = [], $INSERT = SQL::INSERT): array
    {
        if (\is_array($table)) {
            $datas = $table[SQL::VALUES] ?? $table[SQL::SET];
            $table = $this->getTable($table, SQL::INSERT);

            if (\is_string($datas)) {
                $INSERT = $datas;
            }
        }

        if (\is_string($INSERT) && \strpos($INSERT, ' ') !== false) {
            $INSERT = \array_map('\trim', \explode(' ', $INSERT));
        }

        if (\is_array($INSERT)) {
            $do = \in_array(SQL::REPLACE, $INSERT, true) ? 'REPLACE' : 'INSERT';
            $parts = [$do];

            $subs = [
                SQL::LOW_PRIORITY => 'LOW_PRIORITY',
                SQL::DELAYED => 'DELAYED',
            ];
            if ($do === SQL::INSERT) {
                $subs[SQL::HIGH_PRIORITY] = 'HIGH_PRIORITY';
            }
            foreach ($subs as $k => $sub) {
                if (\in_array($k, $INSERT, true)) {
                    $parts[] = $sub;
                    break;
                }
            }

            if ($do === SQL::INSERT && \in_array(SQL::IGNORE, $INSERT, true)) {
                $parts[] = 'IGNORE';
            }

            $INSERT = \implode(' ', $parts);
        } else {
            $INSERT = $INSERT === SQL::REPLACE ? 'REPLACE' : 'INSERT';
        }

        // Check indexed or associative array
        if (!isset($datas[0])) {
            $datas = [$datas];
        }

        $columns = \array_keys($datas[0]);

        $stack = [];
        $this->map = [];

        foreach ($datas as $data) {
            if (\array_keys($data) !== $columns) {
                throw new \InvalidArgumentException('Some rows contain inconsistencies');
            }

            $values = [];
            foreach ($columns as $key) {
                if ($raw = $this->buildRaw($data[$key])) {
                    $values[] = $raw;
                    continue;
                }

                $values[] = $mapKey = $this->mapKey();

                if (!isset($data[$key])) {
                    $this->map[$mapKey] = [null, PDO::PARAM_NULL];
                } else {
                    $value = $data[$key];
                    $this->map[$mapKey] = $this->typeMap($value);
                }

            }

            $stack[] = '(' . \implode(', ', $values) . ')';
        }

        $columns = \array_map([$this, 'columnQuote'], $columns);

        $this->sql = $INSERT . ' INTO ' . $this->tableQuote($table) .
            ' (' . \implode(', ', $columns) . ') VALUES ' . \implode(', ', $stack);

        return [$this->sql, $this->map];
    }

    /**
     * @param                $table
     * @param array $data
     * @param array|Raw|null $where
     *
     * @return array
     */
    public function update($table, array $data = [], $where = null): array
    {
        if (\is_array($table)) {
            $data = $table[SQL::SET] ?? $table[SQL::VALUES];
            $where = $table;
            $table = $this->getTable($table, SQL::UPDATE);
        } elseif ($where) {
            $where = [SQL::WHERE => $where];
        }

        $fields = [];
        $this->map = [];

        foreach ($data as $key => $value) {
            \preg_match('/(?<column>\w+)(\[(?<operator>\+|\-|\*|\/)\])?/i', $key, $match);

            $column = $this->columnQuote($match['column']);
            if ($raw = $this->buildRaw($value)) {
                $fields[] = $column . ' = ' . $raw;
                continue;
            }

            if (isset($match['operator'])) {
                if (\is_numeric($value)) {
                    $fields[] = $column . ' = ' . $column . ' ' . $match['operator'] . ' ' . $value;
                }
            } else {
                $mapKey = $this->mapKey();
                $fields[] = $column . ' = ' . $mapKey;
                $this->map[$mapKey] = $this->typeMap($value);
            }
        }

        $this->sql = 'UPDATE ' . $this->tableQuote($table) . ' SET ' . \implode(', ', $fields) .
            $this->whereClause($where);

        return [$this->sql, $this->map];
    }

    /**
     * @param string|array $table
     * @param array|Raw|null $where
     *
     * @return array
     */
    public function delete($table, $where = null): array
    {
        $this->map = [];
        if (\is_array($table)) {
            $where = $table;
            $table = $this->getTable($table, SQL::DELETE);
        } elseif ($where) {
            $where = [SQL::WHERE => $where];
        }

        $this->sql = 'DELETE FROM ' . $this->tableQuote($table) . $this->whereClause($where);

        return [$this->sql, $this->map];
    }

    /**
     * @param array|string $table
     * @param array|null $columns
     * @param array|Raw|null $where
     *
     * @return array|null
     */
    public function replace($table, array $columns = null, array $where = null): ?array
    {
        if (\is_array($table)) {
            $where = $table;

            $table = $where[SQL::UPDATE] ?? $where[SQL::TABLE] ?? null;
            if ($table === null) {
                throw new \InvalidArgumentException('SQL must contains table.');
            }

            $columns = $where[SQL::SET] ?? $where[SQL::VALUES] ?? null;
            if ($columns === null) {
                throw new \InvalidArgumentException('SQL must contains columns');
            }

        } elseif ($where) {
            $where = [SQL::WHERE => $where];
        }

        if (!\is_array($columns) || empty($columns)) {
            return null;
        }

        $this->map = [];
        $stack = [];

        foreach ($columns as $column => $replacements) {
            if (\is_array($replacements)) {
                foreach ($replacements as $old => $new) {
                    $mapKey = $this->mapKey();

                    $stack[] = $this->columnQuote($column) . ' = REPLACE(' . $this->columnQuote($column) . ', ' . $mapKey . 'a, ' . $mapKey . 'b)';

                    $this->map[$mapKey . 'a'] = [$old, PDO::PARAM_STR];
                    $this->map[$mapKey . 'b'] = [$new, PDO::PARAM_STR];
                }
            }
        }

        if ($stack === []) {
            return null;
        }

        $this->sql = 'UPDATE ' . $this->tableQuote($table) . ' SET ' .
            \implode(', ', $stack) . $this->whereClause($where);

        return [$this->sql, $this->map];
    }

    /**
     * @param array $struct
     *
     * @return array
     */
    public function has(array $struct): array
    {
        unset($struct[SQL::COLUMNS], $struct[SQL::SELECT]);

        $struct[SQL::FUN] = 1;
        $this->select($struct);

        $this->sql = 'SELECT EXISTS(' . $this->sql . ')';

        return [$this->sql, $this->map];
    }

    /**
     * @param string $table
     *
     * @return array
     */
    public function truncate(string $table): array
    {
        $table = $this->tableQuote($table);
        if ($this->type === 'sqlite') {
            return [
                'DELETE FROM ' . $table,
                'UPDATE "sqlite_sequence" SET "seq" = 0 WHERE "name" = ' . $table,
            ];
        }

        return ['TRUNCATE TABLE ' . $table];
    }

    public function rand($struct): array
    {
        $struct = $this->selectFormat($struct);

        $order = 'RANDOM()';
        if ($this->type === 'mysql') {
            $order = 'RAND()';
        }

        $struct[SQL::ORDER] = $this->raw($order);

        return $struct;
    }

    public function escape(string $string): string
    {
        return \str_replace(
            ['\\', "\x00", "\n", "\r", '\'', '"', "\x1a"],
            ['\\\\', '\\0', '\n', '\r', '\\\'', '\"', '\Z'],
            $string
        );
    }

    public function generate(string $query, array $map): string
    {
        foreach ($map as $key => $value) {
            if ($value[1] === PDO::PARAM_STR) {
                $replace = '\'' . $this->escape($value[0]) . '\'';
            } elseif ($value[1] === PDO::PARAM_NULL) {
                $replace = 'NULL';
            } elseif ($value[1] === PDO::PARAM_LOB) {
                $replace = '\'' . $this->escape(
                        \stream_get_contents($value[0])
                    ) . '\'';
            } else {
                $replace = $value[0];
            }

            $query = \str_replace($key, $replace, $query);
        }

        return $query;
    }

    public function __toString(): string
    {
        if (empty($this->sql)) {
            return '';
        }

        return $this->generate($this->sql, $this->map);
    }
}
