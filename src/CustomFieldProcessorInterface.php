<?php

declare(strict_types=1);

namespace Netlogix\Doctrine\Upsert;

interface CustomFieldProcessorInterface
{
    public function processField(string $table, string $column, mixed $value): mixed;

    public function postUpsert(string $table, array $allFields, int $id): void;
}
