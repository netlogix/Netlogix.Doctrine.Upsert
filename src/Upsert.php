<?php
declare(strict_types=1);

namespace Netlogix\Doctrine\Upsert;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Netlogix\Doctrine\Upsert\Exception;

class Upsert
{

    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var string
     */
    private $table;

    /**
     * @var array
     */
    private $identifiers = [];

    /**
     * @var array
     */
    private $fields = [];

    private function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function forTable(string $table): self
    {
        $this->table = $table;

        return $this;
    }

    public function withIdentifier(string $column, $value, int $parameterType = ParameterType::STRING): self
    {
        if (array_key_exists($column, $this->identifiers)) {
            throw new Exception\IdentifierAlreadyInUse(sprintf('The identifier "%s" has already been set!', $column), 1603196381);
        }
        if (array_key_exists($column, $this->fields)) {
            throw new Exception\IdentifierRegisteredAsField(sprintf('The identifier "%s" has already been set as field!', $column), 1603197666);
        }

        $this->identifiers[$column] = [
            'value' => $value,
            'type' => $parameterType,
        ];

        return $this;
    }

    public function withField(string $column, $value, int $parameterType = ParameterType::STRING): self
    {
        if (array_key_exists($column, $this->fields)) {
            throw new Exception\FieldAlreadyInUse(sprintf('The field "%s" has already been set!', $column), 1603196457);
        }
        if (array_key_exists($column, $this->identifiers)) {
            throw new Exception\FieldRegisteredAsIdentifier(sprintf('The field "%s" has already been set as identifier!', $column), 1603197691);
        }

        $this->fields[$column] = [
            'value' => $value,
            'type' => $parameterType,
        ];

        return $this;
    }

    public function execute(): void
    {
        if (!$this->table) {
            throw new Exception\NoTableGiven('No table name has been set!', 1603199471);
        }
        if (count($this->identifiers) === 0 || count($this->fields) === 0) {
            throw new Exception\EmptyUpsert('No columns have been specified for upsert!', 1603199389);
        }

        $allFields = array_merge($this->fields, $this->identifiers);

        $columns = implode(', ', array_keys($allFields));
        $values = implode(', ', array_map(function(string $column) {
            return ':' . $column;
        }, array_keys($allFields)));

        $updates = implode(', ', array_map(function(string $column) {
            return $column . ' = :' . $column;
        }, array_keys($this->fields)));

        $sql = $this->buildQuery($columns, $values, $updates);

        $this->connection->executeQuery(
            $sql,
            array_combine(array_keys($allFields), array_column($allFields, 'value')),
            array_combine(array_keys($allFields), array_column($allFields, 'type'))
        );
    }

    protected function buildQuery(string $columns, string $values, string $updates): string
    {
        switch($this->connection->getDatabasePlatform()->getName()) {
            case 'mysql':
                return <<<MYSQL
INSERT INTO {$this->table} ({$columns})
VALUES ({$values})
ON DUPLICATE KEY UPDATE {$updates}
MYSQL;
            case 'sqlite':
                $conflictColumns = implode(', ', array_keys($this->identifiers));
                return <<<SQLITE
INSERT INTO {$this->table} ({$columns})
VALUES ({$values})
ON CONFLICT({$conflictColumns}) DO UPDATE SET {$updates}
SQLITE;
            default:
                throw new Exception\UnsupportedDatabasePlatform(
                    sprintf('The database platform %s is not supported!', $this->connection->getDatabasePlatform()->getName()),
                    1603199935
                );
        }
    }

    public static function fromConnection(Connection $connection): self
    {
        return new static($connection);
    }

}
