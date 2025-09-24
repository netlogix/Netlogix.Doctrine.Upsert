<?php

declare(strict_types=1);

namespace Netlogix\Doctrine\Upsert;

use BackedEnum;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\MySqlPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use RuntimeException;
use Stringable;

final class Upsert
{
    private ?string $table = null;

    private array $identifiers = [];

    private array $fields = [];

    /**
     * @phpstan-var array<string, CustomFieldProcessorInterface>
     * @var CustomFieldProcessorInterface[]
     */
    private array $customFieldProcessors = [];

    private function __construct(
        private readonly Connection $connection
    ) {
    }

    public static function fromConnection(Connection $connection): self
    {
        return new self($connection);
    }

    public function forTable(string $table): self
    {
        $this->table = $table;

        return $this;
    }

    public function withCustomFieldProcessor(
        string $identifier,
        CustomFieldProcessorInterface $processor,
    ): self {
        $this->customFieldProcessors[$identifier] = $processor;

        return $this;
    }

    public function withIdentifier(string $column, mixed $value, ParameterType $parameterType = ParameterType::STRING): self
    {
        $this->throwErrorIsColumnExists($column);

        if (is_object($value) && method_exists($value, 'rawType')) {
            $parameterType = $value->rawType();
        }

        $value = $this->getValue($value);

        $this->identifiers[$column] = [
            'value' => $value,
            'type' => $parameterType,
        ];

        return $this;
    }

    public function withField(
        string $column,
        mixed $value,
        ParameterType $parameterType = ParameterType::STRING,
        bool $insertOnly = false,
        ?string $customFieldProcessor = null,
    ): self {
        $this->throwErrorIsColumnExists($column);

        if (is_object($value) && method_exists($value, 'rawType')) {
            $parameterType = $value->rawType();
        }

        $value = $this->getValue($value);

        if ($customFieldProcessor && !array_key_exists($customFieldProcessor, $this->customFieldProcessors)) {
            throw new RuntimeException(
                sprintf('The custom field processor "%s" has not been registered!', $customFieldProcessor),
                1709281944,
            );
        }

        if ($customFieldProcessor) {
            $value = $this->customFieldProcessors[$customFieldProcessor]->processField(
                $this->table,
                $column,
                $value,
            );
        }

        $this->fields[$column] = [
            'value' => $value,
            'type' => $parameterType,
            'insertOnly' => $insertOnly,
        ];

        return $this;
    }

    public function execute(): int
    {
        if ($this->table === null) {
            throw new Exception\NoTableGiven('No table name has been set!', 1603199471);
        }

        if ($this->identifiers === [] || $this->fields === []) {
            throw new Exception\EmptyUpsert('No columns have been specified for upsert!', 1603199389);
        }

        $platform = $this->connection->getDatabasePlatform();

        $quotedIdentifiers = implode(', ', array_map(fn(string $col) => $platform->quoteSingleIdentifier($col), array_keys($this->identifiers)));

        $allFields = array_merge($this->fields, $this->identifiers);

        $columns = implode(', ', array_map(fn(string $column) => $platform->quoteSingleIdentifier($column), array_keys($allFields)));
        $values = implode(', ', array_map(static fn (string $column): string => ':' . $column, array_keys($allFields)));

        $updates = implode(
            ', ',
            array_map(
                static fn (string $column): string => $platform->quoteSingleIdentifier($column) . ' = :' . $column,
                array_keys(array_filter($this->fields, static fn(array $field): bool => !$field['insertOnly']))
            )
        );

        $sql = $this->buildQuery($quotedIdentifiers, $columns, $values, $updates);

        $result = $this->connection->executeQuery(
            $sql,
            array_combine(array_keys($allFields), array_column($allFields, 'value')),
            array_combine(array_keys($allFields), array_column($allFields, 'type'))
        );

        if ($this->customFieldProcessors !== []) {
            $id = (int)$this->connection->lastInsertId();
            foreach ($this->customFieldProcessors as $processor) {
                $processor->postUpsert($this->table, $allFields, $id);
            }
        }

        return $result->rowCount();
    }

    protected function buildQuery(string $identifiers, string $columns, string $values, string $updates): string
    {
        $platform = $this->connection->getDatabasePlatform();

        return match (true) {
            $platform instanceof PostgreSQLPlatform => <<<POSTGRESQL
                INSERT INTO "{$this->table}" ({$columns})
                VALUES ({$values})
                ON CONFLICT ({$identifiers}) DO UPDATE SET {$updates}
                POSTGRESQL
        ,
            $platform instanceof AbstractMySQLPlatform || $platform instanceof MySqlPlatform => <<<MYSQL
                INSERT INTO {$this->table} ({$columns})
                VALUES ({$values})
                ON DUPLICATE KEY UPDATE {$updates}
                MYSQL
        ,
            $platform instanceof SqlitePlatform => <<<SQLITE
                INSERT INTO {$this->table} ({$columns})
                VALUES ({$values})
                ON CONFLICT({$identifiers}) DO UPDATE SET {$updates}
                SQLITE
        ,
            default => throw new RuntimeException(
                sprintf('The database platform %s is not supported!', $platform::class),
                1_603_199_935
            )
        };
    }

    private function getValue(mixed $value): int|string|float|bool|null
    {
        if (is_array($value)) {
            $value = json_encode($value, JSON_THROW_ON_ERROR);
        }

        if ($value instanceof Stringable) {
            $value = (string) $value;
        }

        if ($value instanceof BackedEnum) {
            $value = (string) $value->value;
        }

        if (is_object($value) && method_exists($value, 'rawValue')) {
            return $value->rawValue();
        }

        return $value;
    }

    private function throwErrorIsColumnExists(string $column): void
    {
        if (array_key_exists($column, $this->fields)) {
            throw new Exception\FieldAlreadyInUse(sprintf('The field "%s" has already been set!', $column), 1603196457);
        }

        if (array_key_exists($column, $this->identifiers)) {
            throw new Exception\IdentifierRegisteredAsField(
                sprintf('The field "%s" has already been set as identifier!', $column),
                1603197691
            );
        }
    }
}
