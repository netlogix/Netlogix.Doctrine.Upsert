<?php

declare(strict_types=1);

namespace Netlogix\Doctrine\Upsert\Test;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Result;
use PHPUnit\Framework\Attributes\Test;
use Doctrine\DBAL\ParameterType;
use Netlogix\Doctrine\Upsert\Exception;
use Netlogix\Doctrine\Upsert\Upsert;
use PHPUnit\Framework\TestCase;

class UpsertTest extends TestCase
{
    #[Test]
    public function If_no_table_name_has_been_set_an_exception_is_thrown(): void
    {
        self::expectException(Exception\NoTableGiven::class);

        Upsert::fromConnection($this->getMockConnection())
            ->execute();
    }

    #[Test]
    public function If_no_columns_are_specified_an_exception_is_thrown(): void
    {
        self::expectException(Exception\EmptyUpsert::class);

        Upsert::fromConnection($this->getMockConnection())
            ->forTable('foo_table')
            ->execute();
    }

    #[Test]
    public function Identifiers_cannot_be_registered_more_than_once(): void
    {
        self::expectException(Exception\IdentifierRegisteredAsField::class);

        Upsert::fromConnection($this->getMockConnection())
            ->forTable('foo_table')
            ->withIdentifier('bar', 0)
            ->withIdentifier('bar', 0)
            ->execute();
    }

    #[Test]
    public function Identifiers_cannot_be_reregistered_as_fields(): void
    {
        self::expectException(Exception\FieldAlreadyInUse::class);

        Upsert::fromConnection($this->getMockConnection())
            ->forTable('foo_table')
            ->withField('bar', 0)
            ->withIdentifier('bar', 0)
            ->execute();
    }

    #[Test]
    public function Fields_cannot_be_registered_more_than_once(): void
    {
        self::expectException(Exception\FieldAlreadyInUse::class);

        Upsert::fromConnection($this->getMockConnection())
            ->forTable('foo_table')
            ->withField('bar', 0)
            ->withField('bar', 0)
            ->execute();
    }

    #[Test]
    public function Fields_cannot_be_reregistered_as_identifiers(): void
    {
        self::expectException(Exception\IdentifierRegisteredAsField::class);

        Upsert::fromConnection($this->getMockConnection())
            ->forTable('foo_table')
            ->withIdentifier('bar', 0)
            ->withField('bar', 0)
            ->execute();
    }

    #[Test]
    public function Parameters_are_built_correctly(): void
    {
        $connection = $this->getMockConnection();

        $connection
            ->expects(self::once())
            ->method('executeQuery')
            ->with(
                self::anything(),
                [
                    'foo' => 1,
                    'bar' => 2,
                    'baz' => 3,
                    'boo' => 4,
                ],
                self::anything()
            )
            ->willReturn($this->getMockResult(0));

        Upsert::fromConnection($connection)
            ->forTable('foo_table')
            ->withIdentifier('foo', 1)
            ->withIdentifier('bar', 2)
            ->withField('baz', 3)
            ->withField('boo', 4)
            ->execute();
    }

    #[Test]
    public function ParameterTypes_are_built_correctly(): void
    {
        $connection = $this->getMockConnection();

        $connection
            ->expects(self::once())
            ->method('executeQuery')
            ->with(
                self::anything(),
                self::anything(),
                [
                    'foo' => ParameterType::INTEGER,
                    'bar' => ParameterType::STRING,
                    'baz' => ParameterType::BOOLEAN,
                    'boo' => ParameterType::LARGE_OBJECT,
                ]
            )
            ->willReturn($this->getMockResult(0));

        Upsert::fromConnection($connection)
            ->forTable('foo_table')
            ->withIdentifier('foo', 1, ParameterType::INTEGER)
            ->withIdentifier('bar', '2', ParameterType::STRING)
            ->withField('baz', true, ParameterType::BOOLEAN)
            ->withField('boo', 4, ParameterType::LARGE_OBJECT)
            ->execute();
    }

    #[Test]
    public function RowCount_is_returned(): void
    {
        $connection = $this->getMockConnection();

        $connection
            ->expects(self::once())
            ->method('executeQuery')
            ->with(
                self::anything(),
                self::anything(),
                [
                    'foo' => ParameterType::INTEGER,
                    'bar' => ParameterType::STRING,
                    'baz' => ParameterType::BOOLEAN,
                    'boo' => ParameterType::LARGE_OBJECT,
                ]
            )
            ->willReturn($this->getMockResult(35));

        $count = Upsert::fromConnection($connection)
            ->forTable('foo_table')
            ->withIdentifier('foo', 1, ParameterType::INTEGER)
            ->withIdentifier('bar', '2', ParameterType::STRING)
            ->withField('baz', true, ParameterType::BOOLEAN)
            ->withField('boo', 4, ParameterType::LARGE_OBJECT)
            ->execute();

        self::assertEquals(35, $count);
    }

    #[Test]
    public function Table_Name_is_used_in_Query(): void
    {
        self::expectException(DBALException\TableNotFoundException::class);

        Upsert::fromConnection($this->getSQLiteConnection())
            ->forTable('foo_table')
            ->withIdentifier('bar', 0)
            ->withField('baz', 1)
            ->execute();
    }

    #[Test]
    public function Insert_works(): void
    {
        $connection = $this->getSQLiteConnection();
        $connection->exec('CREATE TABLE foo_table(bar TEXT PRIMARY KEY, count INT);');

        Upsert::fromConnection($connection)
            ->forTable('foo_table')
            ->withIdentifier('bar', 'baz')
            ->withField('count', 1)
            ->execute();

        $values = $connection->fetchAll('SELECT * FROM foo_table');
        self::assertCount(1, $values);
        self::assertSame(
            [
                'bar' => 'baz',
                'count' => '1'
            ],
            $values[0]
        );
    }

    #[Test]
    public function Upsert_works(): void
    {
        $connection = $this->getSQLiteConnection();
        $connection->exec('CREATE TABLE foo_table(bar TEXT PRIMARY KEY, count INT);');
        $connection->exec('INSERT INTO foo_table (bar, count) VALUES ("baz", 1)');

        Upsert::fromConnection($connection)
            ->forTable('foo_table')
            ->withIdentifier('bar', 'baz')
            ->withField('count', 2)
            ->execute();

        $values = $connection->fetchAll('SELECT * FROM foo_table');
        self::assertCount(1, $values);
        self::assertSame(
            [
                'bar' => 'baz',
                'count' => '2'
            ],
            $values[0]
        );
    }

    private function getSQLiteConnection(): Connection
    {
        if (!extension_loaded('sqlite')) {
            self::markTestSkipped('ext-sqlite3 is required for tests');
        }

        return DriverManager::getConnection([
            'url' => 'sqlite:///:memory:'
        ]);
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject|Connection
     */
    private function getMockConnection()
    {
        $connection = $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->getMock();

        $platform = $this->getMockBuilder(SQLitePlatform::class)
            ->getMock();

        $connection
            ->method('getDatabasePlatform')
            ->willReturn($platform);

        return $connection;
    }

    private function getMockResult(int $rowCount): Result
    {
        $result = $this->getMockBuilder(Result::class)
            ->disableOriginalConstructor()
            ->getMock();

        $result
            ->method('rowCount')
            ->willReturn($rowCount);

        return $result;
    }

}
