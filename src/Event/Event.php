<?php
namespace Hail\Database\Event;

use Hail\Database\Database;

class Event
{
	/** event type */
	public const CONNECT = 'CONNECT',
		SELECT = 'SELECT',
		INSERT = 'INSERT',
		DELETE = 'DELETE',
		UPDATE = 'UPDATE',
		TRUNCATE = 'TRUNCATE',
		QUERY = 'QUERY', // SELECT | INSERT | DELETE | UPDATE
		TRANSACTION = 'TRANSACTION', // BEGIN | COMMIT | ROLLBACK
        SET = 'SET',
		ALL = 'ALL';

	/** @var string */
	protected $type;

	/** @var string */
	protected $sql;

	/** @var mixed */
	protected $result;

	/** @var float[] */
	protected $time;

	/** @var int */
	protected $count;

	protected $error;

    /**
     * @var Database
     */
	protected $db;

	public function __construct(Database $db, string $type = self::ALL)
	{
		$start = -\microtime(true);
		$this->time = [
			'build' => $start,
			'query' => $start,
			'elapsed' => $start,
		];

		$this->db = $db;

		$this->type = $type;
	}

	public function error(array $error): void
	{
		$this->error = $error;
	}

	public function sql(string $sql, bool $build = true): void
	{
		$time = \microtime(true);

		if ($build) {
			$this->time['build'] += $time;
		}

		$this->time['query'] = -$time;
		$this->sql = $sql;

		if ($this->type === self::QUERY &&
			\preg_match('#\(?\s*(SELECT|UPDATE|INSERT|REPLACE INTO|DELETE|TRUNCATE|SET)#iA', $sql, $matches)
		) {
		    $type = \strtoupper($matches[1]);
		    if ($type === 'REPLACE INTO') {
		        $type = 'INSERT';
            }

			$this->type = $type;
		}
	}

	public function query(): void
	{
		$time = \microtime(true);
		$this->time['query'] += $time;
		$this->time['fetch'] = -$time;
	}

	public function done($result = null): void
	{
		$time = \microtime(true);

		$this->result = $result;

        $this->count = 0;
		if ($result) {
            $this->count = \count((array) $result);
        }

		if (isset($this->time['fetch'])) {
			$this->time['fetch'] += $time;
		}

		if ($this->time['query'] < 0) {
			$this->time['query'] += $time;
		}

		$this->time['elapsed'] += $time;

		Collector::add($this);
	}

	public function isError(): bool
	{
		return $this->error !== null;
	}

	public function getResult()
	{
		return $this->error ?? $this->result ?? null;
	}

	public function getStorageType(): string
	{
		return $this->db->getType();

	}

	public function getDatabaseName(): string
	{
		return $this->db->getDatabase();
	}

	/**
	 * Actual formatted query, e.g. 'SELECT * FROM ...'
	 *
	 * @return string
	 */
	public function getQuery(): string
	{
		return $this->dump($this->sql);
	}

	/**
	 * @return NULL|float ms
	 */
	public function getBuildTime(): ?float
	{
		return $this->time['build'] > 0 ? $this->time['build'] * 1000 : null;
	}

	/**
	 * @return NULL|float ms
	 */
	public function getQueryTime(): ?float
	{
		return $this->time['query'] > 0 ? $this->time['query'] * 1000 : null;
	}

	/**
	 * @return NULL|float ms
	 */
	public function getFetchTime(): ?float
	{
		return isset($this->time['fetch']) ? $this->time['fetch'] * 1000 : null;
	}

	/**
	 * @return NULL|float ms
	 */
	public function getElapsedTime(): ?float
	{
		return $this->time['elapsed'] * 1000;
	}

	public function getType(): string
	{
		return $this->type;
	}

	/**
	 * e.g. SQL explain
	 */
	public function getInfo(): ?array
	{
		if ($this->error || !$this->sql) {
			return null;
		}

		$query = 'EXPLAIN FORMAT=JSON ' . $this->sql;
		$query = $this->db->getPdo()->query($query);

		$data = [];
		if ($query) {
			$data = $query->fetch();
		}

		return \json_decode($data['EXPLAIN'] ?? '[]', true);
	}

	/**
	 * Prints out a syntax highlighted version of the SQL command or Result.
	 *
	 * @param  string $result
	 *
	 * @return string
	 */
	public function dump(string $result = null): string
	{
		static $keywords1 = 'SELECT|(?:ON\s+DUPLICATE\s+KEY)?UPDATE|INSERT(?:\s+INTO)?|REPLACE(?:\s+INTO)?|DELETE|CALL|UNION|FROM|WHERE|HAVING|GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET|FETCH\s+NEXT|SET|VALUES|LEFT\s+JOIN|INNER\s+JOIN|TRUNCATE|START\s+TRANSACTION|BEGIN|COMMIT|ROLLBACK(?:\s+TO\s+SAVEPOINT)?|(?:RELEASE\s+)?SAVEPOINT';
		static $keywords2 = 'ALL|DISTINCT|DISTINCTROW|IGNORE|AS|USING|ON|AND|OR|IN|IS|NOT|NULL|LIKE|RLIKE|REGEXP|TRUE|FALSE';

		// insert new lines
		$sql = " $result ";
		$sql = \preg_replace("#(?<=[\\s,(])($keywords1)(?=[\\s,)])#i", "\n\$1", $sql);
		// reduce spaces
		$sql = \preg_replace('#[ \t]{2,}#', ' ', $sql);
		$sql = \wordwrap($sql, 100);
		$sql = \preg_replace("#([ \t]*\r?\n){2,}#", "\n", $sql);
		// syntax highlight
		$highlighter = "#(/\\*.+?\\*/)|(\\*\\*.+?\\*\\*)|(?<=[\\s,(])($keywords1)(?=[\\s,)])|(?<=[\\s,(=])($keywords2)(?=[\\s,)=])#is";

		$sql = \htmlspecialchars($sql);
		$sql = \preg_replace_callback($highlighter, static function ($m) {
			if (!empty($m[1])) { // comment
				return '<em style="color:gray">' . $m[1] . '</em>';
			}

			if (!empty($m[2])) { // error
				return '<strong style="color:red">' . $m[2] . '</strong>';
			}

			if (!empty($m[3])) { // most important keywords
				return '<strong style="color:blue">' . $m[3] . '</strong>';
			}

			if (!empty($m[4])) { // other keywords
				return '<strong style="color:green">' . $m[4] . '</strong>';
			}

			return '';
		}, $sql);

		return \trim($sql);
	}
}
